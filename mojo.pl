#!/usr/bin/env perl

use v5.20.0;
use strict;
use warnings;

use Data::Dumper;
use Getopt::Long;
use Log::Log4perl;
use Mojo;
use Mojo::IOLoop;
use Mojo::IOLoop::Stream;
use POSIX 'floor';
use Time::HiRes 'time';

$|=1;

my $usage = "

  cat <logfile> | $0 <options>

  OR

  curl -s 'https://logs.forge.bbc.co.uk/tail/tail/live/service-app-live-app-logs/httpd-api-gw/access_log?lines=5' | $0 <options>

  <options>
  
  --host (default = http://localhost/)
    Host to forward requests to. Specify multiple --host options to forward to
    multiple hosts on a round robin basis.
  --path (default = /nitro/api)
    Path that should be used.
  --key
    Nitro API key (outgoing requests will have api_key rewritten to this value).
  --lag=<N>
    Wait at least <N> seconds before forwarding requests.
  --load (default 1)
    Load multiplier. Set to e.g. 0.1 to forward 10% of requests.
  --maxrate=<requests-per-second>
    Maximum number of requests to forward per second (irrespective of --load).
  --cert
    Path to client certificate (.pem) to use when forwarding requests.
  --limit
    If defined, exit after this many requests have been replayed.
  --timeout (default = 10)
    Quit after this many seconds of inactivity on the input stream.
";

my $level = $ENV{MOJO_METER_LOG_LEVEL} || 'INFO';

Log::Log4perl->init(\qq{
  log4perl.logger = $level,Screen
  log4perl.appender.Screen=Log::Log4perl::Appender::ScreenColoredLevels
  log4perl.appender.Screen.color.DEBUG=cyan
  log4perl.appender.Screen.layout=Log::Log4perl::Layout::PatternLayout
  log4perl.appender.Screen.layout.ConversionPattern=[%d] %p %P : %C ln %L: %m%n
});

my $log = Log::Log4perl::get_logger;

my $opt = {
  host => [],
  path => '/nitro/api',
  key => 'abc123',
  lag => 0,
  maxrate => 200,
  load => 1,
  cert => undef,
  limit => undef,
  timeout => 10
};

GetOptions(
  $opt,
  'host=s@',
  'path=s',
  'key=s',
  'lag=i',
  'load=f',
  'maxrate=i',
  'cert=s',
  'limit=i',
  'timeout=i',
) or die $usage;
$opt->{host} = ['http://localhost'] unless scalar(@{$opt->{host}});

my $urls = {};
my $host_n = 0;
my $received = 0;
my $replayed = 0;
my $skipped = 0;
my $start = time();

my $stream = Mojo::IOLoop::Stream->new(*STDIN);
$stream->timeout($opt->{timeout});
$stream->on( read => sub {
  my( $stream, $bytes ) = @_;

  my @lines = split("\n", $bytes);
  return unless @lines;

  for my $line (@lines) {
    my( $path ) = ( $line !~ m{/nitro/api/v1/} && $line =~ m{(/nitro/api/\S+)} );
    unless($path) {
      $skipped++;
      next;
    }

    # rewrite path if appropriate
    if ($opt->{path} ne '/nitro/api') {
      $path =~ s:/nitro/api:$opt->{path}:;
    }

    # rewrite api key in path
    $path =~ s/api_key=([a-zA-Z0-9]+)/api_key=$opt->{key}/;

    my $url = $opt->{host}->[$host_n] . $path;

    my $now = time();
    $urls->{$now} ||= [];
    push @{ $urls->{$now} }, $url;
    $host_n++;
    $host_n = 0 if $host_n >= scalar(@{$opt->{host}});
    $received++;
    Mojo::IOLoop->stop if $opt->{limit} && $received >= $opt->{limit};
  }
});
$stream->on( timeout => sub { 
    $stream->close_gracefully(); 
});

my $ua = Mojo::UserAgent->new;
$ua->proxy->detect;
$ua->cert($opt->{cert}) if $opt->{cert};
$ua->request_timeout(0);
$ua->inactivity_timeout(0);

Mojo::IOLoop->recurring( 1 => sub {
  my $elapsed = time() - $start;
  my $rate_received = $elapsed ? sprintf( '%.2f', $received / $elapsed ) : 0;
  my $rate_replayed = $elapsed ? sprintf( '%.2f', $replayed / $elapsed ) : 0;
  $log->info( sprintf(
    "Elapsed: %s, Received: %s (%s/s), Skipped: %s, Replayed: %s (%s/s)",
    $elapsed,
    $received,
    $rate_received,
    $skipped,
    $replayed,
    $rate_replayed
  ) );
});

# make requests
Mojo::IOLoop->recurring( 1 => sub {
  my @fetch;
  for my $ts ( sort keys %$urls ) {
    if( $opt->{lag} && time() - $ts < $opt->{lag} ) {
      next;
    }
    my $urls = delete $urls->{$ts};
    push @fetch, @$urls;
  }

  # increase/decrease size of @fetch by N% if load is something other than 1
  if( $opt->{load} && $opt->{load} != 1 ) {
    my $target = floor(scalar @fetch * $opt->{load});
    my @newfetch;
    my $i = 0;
    while( $target-- ) {
      push @newfetch, $fetch[$i];
      $i++;
      $i = 0 if $i > $#fetch;
    }
    @fetch = @newfetch;
  }

  # limit maximum request rate to $opt->{maxrate} per second if set
  splice(@fetch, $opt->{maxrate}) if $opt->{maxrate} && $opt->{maxrate} < scalar @fetch;

  # fetch urls
  my $delay = Mojo::IOLoop->delay(sub {
    my ($delay, @results) = @_;
    for my $result (@results) {
      return unless $result;
      my( $tx, $start ) = @$result;
      my $end = time();
      my $duration = sprintf( "%2f", $end - $start );
      $replayed++;
      my $url = $tx->req->url;
      if( $tx->res->code ) {
        $log->debug("RESPONSE: $start $end $duration " . $tx->res->code . " $url");
      }
      elsif( $tx->error ) {
        my $res_code = defined $tx->res->code ? $tx->res->code : "UNDEF";
        $log->error("ERROR: $start $end $duration $res_code $url " . $tx->error->{message});
      }
    }
  });
  for my $url (@fetch) {
    my $end = $delay->begin(0);
    my $start_time = time();
    $ua->get($url => sub {
      my ($ua, $tx) = @_;
      $end->([$tx, $start_time]);
    });
  }
  $delay->wait;
});

$stream->start;
$stream->reactor->start unless $stream->reactor->is_running;
