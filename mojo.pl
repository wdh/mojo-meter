#!/usr/bin/env perl

use strict;
use warnings;

use Data::Dumper;
use Getopt::Long;
use Mojo;
use Mojo::IOLoop;
use Mojo::IOLoop::Stream;
use POSIX 'floor';
use Time::HiRes 'time';

$|=1;

my $usage = "

  cat <logfile> | $0 <options>

  <options>
  
  --host (default = http://localhost/)
    Host to forward requests to. Specify multiple --host options to forward to
    multiple hosts on a round robin basis.
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
";

my $opt = {
  host => [],
  key => 'abc123',
  lag => 0,
  maxrate => 200,
  load => 1,
  cert => undef,
  limit => undef
};

GetOptions(
  $opt,
  'host=s@',
  'key=s',
  'lag=i',
  'load=f',
  'maxrate=i',
  'cert=s',
  'limit=i',
) or die $usage;
$opt->{host} = ['http://localhost'] unless scalar(@{$opt->{host}});

my $urls = {};
my $host_n = 0;
my $received = 0;
my $replayed = 0;
my $skipped = 0;
my $start = time();

my $stream = Mojo::IOLoop::Stream->new(*STDIN);
$stream->on( read => sub {
  my( $stream, $bytes ) = @_;

  my @lines = split("\n", $bytes);
  return unless @lines;

  for my $line (@lines) {
    my( $path ) = ( $line =~ m{(/nitro/api/[^v1]\S+)} );
    unless($path) {
      $skipped++;
      next;
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

my $ua = Mojo::UserAgent->new;
$ua->proxy->detect;
$ua->cert($opt->{cert}) if $opt->{cert};

Mojo::IOLoop->recurring( 1 => sub {
  my $elapsed = time() - $start;
  my $rate_received = $elapsed ? sprintf( '%.2f', $received / $elapsed ) : 0;
  my $rate_replayed = $elapsed ? sprintf( '%.2f', $replayed / $elapsed ) : 0;
  print sprintf(
    "Elapsed: %s, Received: %s (%s/s), Skipped: %s, Replayed: %s (%s/s)",
    $elapsed,
    $received,
    $rate_received,
    $skipped,
    $replayed,
    $rate_replayed
  ) . "\n";
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
#        print "$start $end $duration " . $tx->res->code . ' ' . $tx->req->url . "\n";
      }
      elsif( $tx->error ) {
#        print time() . " ERROR fetching $url: " . $tx->error->{message} . "\n";
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
