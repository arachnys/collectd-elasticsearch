#! /usr/bin/python

import collectd
import json
import urllib2
import socket

global URL, STAT, PREFIX, ES_HOST, ES_PORT, ES_CLUSTER

PREFIX = "elasticsearch"
ES_CLUSTER = "elasticsearch"
ES_HOST = "localhost"
ES_PORT = 9200
VERBOSE_LOGGING = False
STAT=dict()
CLUSTER_STAT=dict()

# INDICES METRICS #
## DOCS
CLUSTER_STAT['indices.docs.count'] = { "type": "gauge", "path": "nodes.%s.indices.docs.count" }
CLUSTER_STAT['indices.docs.deleted'] = { "type": "counter", "path": "nodes.%s.indices.docs.deleted" }

## FLUSH
STAT['indices._all.primaries.flush.total'] = { "type": "counter", "path": "_all.primaries.flush.total" }
STAT['indices._all.primaries.flush.time'] = { "type": "counter", "path": "_all.primaries.flush.total_time_in_millis" }
STAT['indices._all.total.flush.total'] = { "type": "counter", "path": "_all.total.flush.total" }
STAT['indices._all.total.flush.time'] = { "type": "counter", "path": "_all.total.flush.total_time_in_millis" }

## GET
CLUSTER_STAT['indices.get.exists-time'] = { "type": "counter", "path": "nodes.%s.indices.get.exists_time_in_millis" }
CLUSTER_STAT['indices.get.exists-total'] = { "type": "counter", "path": "nodes.%s.indices.get.exists_total" }
CLUSTER_STAT['indices.get.time'] = { "type": "counter", "path": "nodes.%s.indices.get.time_in_millis" }
CLUSTER_STAT['indices.get.total'] = { "type": "counter", "path": "nodes.%s.indices.get.total" }
CLUSTER_STAT['indices.get.missing-time'] = { "type": "counter", "path": "nodes.%s.indices.get.missing_time_in_millis" }
CLUSTER_STAT['indices.get.missing-total'] = { "type": "counter", "path": "nodes.%s.indices.get.missing_total" }

## INDEXING
CLUSTER_STAT['indices.indexing.delete-time'] = { "type": "counter", "path": "nodes.%s.indices.indexing.delete_time_in_millis" }
CLUSTER_STAT['indices.indexing.delete-total'] = { "type": "counter", "path": "nodes.%s.indices.indexing.delete_total" }
CLUSTER_STAT['indices.indexing.index-time'] = { "type": "counter", "path": "nodes.%s.indices.indexing.index_time_in_millis" }
CLUSTER_STAT['indices.indexing.index-total'] = { "type": "counter", "path": "nodes.%s.indices.indexing.index_total" }

## MERGES
STAT['indices._all.primaries.merges.current'] = { "type": "gauge", "path": "_all.primaries.merges.current" }
STAT['indices._all.primaries.merges.current-docs'] = { "type": "gauge", "path": "_all.primaries.merges.current_docs" }
STAT['indices._all.primaries.merges.current-size'] = { "type": "bytes", "path": "_all.primaries.merges.current_size_in_bytes" }
STAT['indices._all.primaries.merges.total'] = { "type": "counter", "path": "_all.primaries.merges.total" }
STAT['indices._all.primaries.merges.total-docs'] = { "type": "gauge", "path": "_all.primaries.merges.total_docs" }
STAT['indices._all.primaries.merges.total-size'] = { "type": "bytes", "path": "_all.primaries.merges.total_size_in_bytes" }
STAT['indices._all.primaries.merges.time'] = { "type": "counter", "path": "_all.primaries.merges.total_time_in_millis" }

STAT['indices._all.total.merges.current'] = { "type": "gauge", "path": "_all.total.merges.current" }
STAT['indices._all.total.merges.current-docs'] = { "type": "gauge", "path": "_all.total.merges.current_docs" }
STAT['indices._all.total.merges.current-size'] = { "type": "bytes", "path": "_all.total.merges.current_size_in_bytes" }
STAT['indices._all.total.merges.total'] = { "type": "counter", "path": "_all.total.merges.total" }
STAT['indices._all.total.merges.total-docs'] = { "type": "gauge", "path": "_all.total.merges.total_docs" }
STAT['indices._all.total.merges.total-size'] = { "type": "bytes", "path": "_all.total.merges.total_size_in_bytes" }
STAT['indices._all.total.merges.time'] = { "type": "counter", "path": "_all.total.merges.total_time_in_millis" }

## REFRESH
STAT['indices._all.primaries.refresh.total'] = { "type": "counter", "path": "_all.primaries.refresh.total" }
STAT['indices._all.primaries.refresh.time'] = { "type": "counter", "path": "_all.primaries.refresh.total_time_in_millis" }
STAT['indices._all.total.refresh.total'] = { "type": "counter", "path": "_all.total.refresh.total" }
STAT['indices._all.total.refresh.time'] = { "type": "counter", "path": "_all.total.refresh.total_time_in_millis" }

## SEARCH
CLUSTER_STAT['indices.search.query-current'] = { "type": "gauge", "path": "nodes.%s.indices.search.query_current" }
CLUSTER_STAT['indices.search.query-total'] = { "type": "counter", "path": "nodes.%s.indices.search.query_total" }
CLUSTER_STAT['indices.search.query-time'] = { "type": "counter", "path": "nodes.%s.indices.search.query_time_in_millis" }
CLUSTER_STAT['indices.search.fetch-current'] = { "type": "counter", "path": "nodes.%s.indices.search.fetch_current" }
CLUSTER_STAT['indices.search.fetch-total'] = { "type": "counter", "path": "nodes.%s.indices.search.fetch_total" }
CLUSTER_STAT['indices.search.fetch-time'] = { "type": "counter", "path": "nodes.%s.indices.search.fetch_time_in_millis" }

## STORE
CLUSTER_STAT['indices.store.size'] = { "type": "bytes", "path": "nodes.%s.indices.store.size_in_bytes" }

# JVM METRICS #
## MEM
CLUSTER_STAT['jvm.mem.heap-committed'] = { "type": "bytes", "path": "nodes.%s.jvm.mem.heap_committed_in_bytes" }
CLUSTER_STAT['jvm.mem.heap-used'] = { "type": "bytes", "path": "nodes.%s.jvm.mem.heap_used_in_bytes" }
CLUSTER_STAT['jvm.mem.non-heap-committed'] = { "type": "bytes", "path": "nodes.%s.jvm.mem.non_heap_committed_in_bytes" }
CLUSTER_STAT['jvm.mem.non-heap-used'] = { "type": "bytes", "path": "nodes.%s.jvm.mem.non_heap_used_in_bytes" }

## THREADS
CLUSTER_STAT['jvm.threads.count'] = { "type": "gauge", "path": "nodes.%s.jvm.threads.count" }
CLUSTER_STAT['jvm.threads.peak'] = { "type": "gauge", "path": "nodes.%s.jvm.threads.peak_count" }

## GC
CLUSTER_STAT['jvm.gc.time'] = { "type": "counter", "path": "nodes.%s.jvm.gc.collection_time_in_millis" }
CLUSTER_STAT['jvm.gc.count'] = { "type": "counter", "path": "nodes.%s.jvm.gc.collection_count" }

# TRANSPORT METRICS #
CLUSTER_STAT['transport.server_open'] = { "type": "gauge", "path": "nodes.%s.transport.server_open" }
CLUSTER_STAT['transport.rx.count'] = { "type": "counter", "path": "nodes.%s.transport.rx_count" }
CLUSTER_STAT['transport.rx.size'] = { "type": "bytes", "path": "nodes.%s.transport.rx_size_in_bytes" }
CLUSTER_STAT['transport.tx.count'] = { "type": "counter", "path": "nodes.%s.transport.tx_count" }
CLUSTER_STAT['transport.tx.size'] = { "type": "bytes", "path": "nodes.%s.transport.tx_size_in_bytes" }

# HTTP METRICS #
CLUSTER_STAT['http.current_open'] = { "type": "gauge", "path": "nodes.%s.http.current_open" }
CLUSTER_STAT['http.total_open'] = { "type": "gauge", "path": "nodes.%s.http.total_opened" }

# PROCESS METRICS #
CLUSTER_STAT['process.open_file_descriptors'] = { "type": "gauge", "path": "nodes.%s.process.open_file_descriptors" }

# THREAD POOL #
## GENERIC
CLUSTER_STAT['thread-pool.generic.threads'] = { "type": "gauge", "path": "nodes.%s.thread_pool.generic.threads" }
CLUSTER_STAT['thread-pool.generic.queue'] = { "type": "gauge", "path": "nodes.%s.thread_pool.generic.queue" }
CLUSTER_STAT['thread-pool.generic.active'] = { "type": "gauge", "path": "nodes.%s.thread_pool.generic.active" }
CLUSTER_STAT['thread-pool.generic.rejected'] = { "type": "gauge", "path": "nodes.%s.thread_pool.generic.rejected" }
CLUSTER_STAT['thread-pool.generic.largest'] = { "type": "gauge", "path": "nodes.%s.thread_pool.generic.largest" }
CLUSTER_STAT['thread-pool.generic.completed'] = { "type": "gauge", "path": "nodes.%s.thread_pool.generic.completed" }

## INDEX
CLUSTER_STAT['thread-pool.index.threads'] = { "type": "gauge", "path": "nodes.%s.thread_pool.index.threads" }
CLUSTER_STAT['thread-pool.index.queue'] = { "type": "gauge", "path": "nodes.%s.thread_pool.index.queue" }
CLUSTER_STAT['thread-pool.index.active'] = { "type": "gauge", "path": "nodes.%s.thread_pool.index.active" }
CLUSTER_STAT['thread-pool.index.rejected'] = { "type": "gauge", "path": "nodes.%s.thread_pool.index.rejected" }
CLUSTER_STAT['thread-pool.index.largest'] = { "type": "gauge", "path": "nodes.%s.thread_pool.index.largest" }
CLUSTER_STAT['thread-pool.index.completed'] = { "type": "gauge", "path": "nodes.%s.thread_pool.index.completed" }

## GET
CLUSTER_STAT['thread-pool.get.threads'] = { "type": "gauge", "path": "nodes.%s.thread_pool.get.threads" }
CLUSTER_STAT['thread-pool.get.queue'] = { "type": "gauge", "path": "nodes.%s.thread_pool.get.queue" }
CLUSTER_STAT['thread-pool.get.active'] = { "type": "gauge", "path": "nodes.%s.thread_pool.get.active" }
CLUSTER_STAT['thread-pool.get.rejected'] = { "type": "gauge", "path": "nodes.%s.thread_pool.get.rejected" }
CLUSTER_STAT['thread-pool.get.largest'] = { "type": "gauge", "path": "nodes.%s.thread_pool.get.largest" }
CLUSTER_STAT['thread-pool.get.completed'] = { "type": "gauge", "path": "nodes.%s.thread_pool.get.completed" }

## SNAPSHOT
CLUSTER_STAT['thread-pool.snapshot.threads'] = { "type": "gauge", "path": "nodes.%s.thread_pool.snapshot.threads" }
CLUSTER_STAT['thread-pool.snapshot.queue'] = { "type": "gauge", "path": "nodes.%s.thread_pool.snapshot.queue" }
CLUSTER_STAT['thread-pool.snapshot.active'] = { "type": "gauge", "path": "nodes.%s.thread_pool.snapshot.active" }
CLUSTER_STAT['thread-pool.snapshot.largest'] = { "type": "gauge", "path": "nodes.%s.thread_pool.snapshot.largest" }
CLUSTER_STAT['thread-pool.snapshot.completed'] = { "type": "gauge", "path": "nodes.%s.thread_pool.snapshot.completed" }

## MERGE
CLUSTER_STAT['thread-pool.merge.threads'] = { "type": "gauge", "path": "nodes.%s.thread_pool.merge.threads" }
CLUSTER_STAT['thread-pool.merge.queue'] = { "type": "gauge", "path": "nodes.%s.thread_pool.merge.queue" }
CLUSTER_STAT['thread-pool.merge.active'] = { "type": "gauge", "path": "nodes.%s.thread_pool.merge.active" }
CLUSTER_STAT['thread-pool.merge.largest'] = { "type": "gauge", "path": "nodes.%s.thread_pool.merge.largest" }
CLUSTER_STAT['thread-pool.merge.completed'] = { "type": "gauge", "path": "nodes.%s.thread_pool.merge.completed" }

## BULK
CLUSTER_STAT['thread-pool.bulk.threads'] = { "type": "gauge", "path": "nodes.%s.thread_pool.bulk.threads" }
CLUSTER_STAT['thread-pool.bulk.queue'] = { "type": "gauge", "path": "nodes.%s.thread_pool.bulk.queue" }
CLUSTER_STAT['thread-pool.bulk.active'] = { "type": "gauge", "path": "nodes.%s.thread_pool.bulk.active" }
CLUSTER_STAT['thread-pool.bulk.rejected'] = { "type": "gauge", "path": "nodes.%s.thread_pool.bulk.rejected" }
CLUSTER_STAT['thread-pool.bulk.largest'] = { "type": "gauge", "path": "nodes.%s.thread_pool.bulk.largest" }
CLUSTER_STAT['thread-pool.bulk.completed'] = { "type": "gauge", "path": "nodes.%s.thread_pool.bulk.completed" }

## WARMER
CLUSTER_STAT['thread-pool.warmer.threads'] = { "type": "gauge", "path": "nodes.%s.thread_pool.warmer.threads" }
CLUSTER_STAT['thread-pool.warmer.queue'] = { "type": "gauge", "path": "nodes.%s.thread_pool.warmer.queue" }
CLUSTER_STAT['thread-pool.warmer.active'] = { "type": "gauge", "path": "nodes.%s.thread_pool.warmer.active" }
CLUSTER_STAT['thread-pool.warmer.largest'] = { "type": "gauge", "path": "nodes.%s.thread_pool.warmer.largest" }
CLUSTER_STAT['thread-pool.warmer.completed'] = { "type": "gauge", "path": "nodes.%s.thread_pool.warmer.completed" }

## FLUSH
CLUSTER_STAT['thread-pool.flush.threads'] = { "type": "gauge", "path": "nodes.%s.thread_pool.flush.threads" }
CLUSTER_STAT['thread-pool.flush.queue'] = { "type": "gauge", "path": "nodes.%s.thread_pool.flush.queue" }
CLUSTER_STAT['thread-pool.flush.active'] = { "type": "gauge", "path": "nodes.%s.thread_pool.flush.active" }
CLUSTER_STAT['thread-pool.flush.largest'] = { "type": "gauge", "path": "nodes.%s.thread_pool.flush.largest" }
CLUSTER_STAT['thread-pool.flush.completed'] = { "type": "gauge", "path": "nodes.%s.thread_pool.flush.completed" }

## SEARCH
CLUSTER_STAT['thread-pool.search.threads'] = { "type": "gauge", "path": "nodes.%s.thread_pool.search.threads" }
CLUSTER_STAT['thread-pool.search.queue'] = { "type": "gauge", "path": "nodes.%s.thread_pool.search.queue" }
CLUSTER_STAT['thread-pool.search.active'] = { "type": "gauge", "path": "nodes.%s.thread_pool.search.active" }
CLUSTER_STAT['thread-pool.search.rejected'] = { "type": "gauge", "path": "nodes.%s.thread_pool.search.rejected" }
CLUSTER_STAT['thread-pool.search.largest'] = { "type": "gauge", "path": "nodes.%s.thread_pool.search.largest" }
CLUSTER_STAT['thread-pool.search.completed'] = { "type": "gauge", "path": "nodes.%s.thread_pool.search.completed" }

## PERCOLATE
CLUSTER_STAT['thread-pool.percolate.threads'] = { "type": "gauge", "path": "nodes.%s.thread_pool.percolate.threads" }
CLUSTER_STAT['thread-pool.percolate.queue'] = { "type": "gauge", "path": "nodes.%s.thread_pool.percolate.queue" }
CLUSTER_STAT['thread-pool.percolate.active'] = { "type": "gauge", "path": "nodes.%s.thread_pool.percolate.active" }
CLUSTER_STAT['thread-pool.percolate.rejected'] = { "type": "gauge", "path": "nodes.%s.thread_pool.percolate.rejected" }
CLUSTER_STAT['thread-pool.percolate.largest'] = { "type": "gauge", "path": "nodes.%s.thread_pool.percolate.largest" }
CLUSTER_STAT['thread-pool.percolate.completed'] = { "type": "gauge", "path": "nodes.%s.thread_pool.percolate.completed" }

## MANAGEMENT
CLUSTER_STAT['thread-pool.management.threads'] = { "type": "gauge", "path": "nodes.%s.thread_pool.management.threads" }
CLUSTER_STAT['thread-pool.management.queue'] = { "type": "gauge", "path": "nodes.%s.thread_pool.management.queue" }
CLUSTER_STAT['thread-pool.management.active'] = { "type": "gauge", "path": "nodes.%s.thread_pool.management.active" }
CLUSTER_STAT['thread-pool.management.largest'] = { "type": "gauge", "path": "nodes.%s.thread_pool.management.largest" }
CLUSTER_STAT['thread-pool.management.completed'] = { "type": "gauge", "path": "nodes.%s.thread_pool.management.completed" }

## REFRESH
CLUSTER_STAT['thread-pool.refresh.threads'] = { "type": "gauge", "path": "nodes.%s.thread_pool.refresh.threads" }
CLUSTER_STAT['thread-pool.refresh.queue'] = { "type": "gauge", "path": "nodes.%s.thread_pool.refresh.queue" }
CLUSTER_STAT['thread-pool.refresh.active'] = { "type": "gauge", "path": "nodes.%s.thread_pool.refresh.active" }
CLUSTER_STAT['thread-pool.refresh.largest'] = { "type": "gauge", "path": "nodes.%s.thread_pool.refresh.largest" }
CLUSTER_STAT['thread-pool.refresh.completed'] = { "type": "gauge", "path": "nodes.%s.thread_pool.refresh.completed" }

# FUNCTION: Collect stats from JSON result
def lookup_stat(stat, json):

    if 'nodes' in json:
        node = json['nodes'].keys()[0]
        val = dig_it_up(json, CLUSTER_STAT[stat]["path"] % node )
    else:
        val = dig_it_up(json, STAT[stat]["path"])

    # Check to make sure we have a valid result
    # dig_it_up returns False if no match found
    if not isinstance(val,bool):
        return int(val)
    else:
        return None

def configure_callback(conf):
    """Received configuration information"""
    global ES_HOST, ES_PORT, ES_CLUSTER_STATS_URL, ES_STATS_URL, ES_STATS_URL, VERBOSE_LOGGING
    for node in conf.children:
        if node.key == 'Host':
            ES_HOST = node.values[0]
        elif node.key == 'Port':
            ES_PORT = int(node.values[0])
        elif node.key == 'Verbose':
            VERBOSE_LOGGING = bool(node.values[0])
        elif node.key == 'Cluster':
            ES_CLUSTER = node.values[0]
        else:
            collectd.warning('elasticsearch plugin: Unknown config key: %s.'
                             % node.key)
    ES_CLUSTER_STATS_URL = "http://" + ES_HOST + ":" + str(ES_PORT) + "/_cluster/nodes/_local/stats?http=true&process=true&jvm=true&transport=true&thread_pool=true"
    ES_STATS_URL = "http://" + ES_HOST + ":" + str(ES_PORT) + "/_stats?clear=true&merge=true&flush=true&refresh=true"

    log_verbose('Configured with host=%s, port=%s, cluster_stats_url=%s, stats_url=%s'
                % (ES_HOST, ES_PORT, ES_CLUSTER_STATS_URL, ES_STATS_URL))

def fetch_stats(): 
    global ES_URL, ES_CLUSTER, ES_CLUSTER_STATS_URL, ES_STATS_URL

    try:
        cluster_results = json.load(urllib2.urlopen(ES_CLUSTER_STATS_URL, timeout = 10))
        stats_results = json.load(urllib2.urlopen(ES_STATS_URL, timeout = 10))
    except urllib2.URLError, e:
        collectd.error('elasticsearch plugin: Error connecting to %s - %r' % (ES_URL, e))
        return None
    print cluster_results['cluster_name']
  
    ES_CLUSTER = cluster_results['cluster_name']
    parse_stats(cluster_results, CLUSTER_STAT)
    parse_stats(stats_results, STAT)
    return None

def parse_stats(json, stat_dict):
    """Parse stats response from ElasticSearch"""
    for name,key in stat_dict.iteritems():
        result = lookup_stat(name, json)
        dispatch_stat(result, name, key)

def dispatch_stat(result, name, key):
    """Read a key from info response data and dispatch a value"""
    if not key.has_key("path"):
        collectd.warning('elasticsearch plugin: Stat not found: %s' % key)
        return
    if result is None:
        collectd.warning('elasticsearch plugin: Value not found for %s' % name)
        return
    type = key["type"]
    value = int(result)
    log_verbose('Sending value[%s]: %s=%s' % (type, name, value))

    val = collectd.Values(plugin='elasticsearch')
    val.plugin_instance = ES_CLUSTER
    val.type = type
    val.type_instance = name
    val.values = [value]
    val.dispatch()

def read_callback():
    log_verbose('Read callback called')
    stats = fetch_stats()

def dig_it_up(obj,path):
    try:
        if type(path) in (str,unicode):
            path = path.split('.')
        return reduce(lambda x,y:x[y],path,obj)
    except:
        return False

def log_verbose(msg):
    if not VERBOSE_LOGGING:
        return
    collectd.info('elasticsearch plugin [verbose]: %s' % msg)

collectd.register_config(configure_callback)
collectd.register_read(read_callback)

