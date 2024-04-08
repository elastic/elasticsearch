/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

import org.elasticsearch.index.codec.tsdb.ES87TSDBDocValuesFormat;
import org.elasticsearch.plugins.internal.RestExtension;

/** The Elasticsearch Server Module. */
module org.elasticsearch.server {
    requires java.logging;
    requires java.security.jgss;
    requires java.sql;
    requires java.management;
    requires jdk.unsupported;
    requires java.net.http; // required by ingest-geoip's dependency maxmind.geoip2 https://github.com/elastic/elasticsearch/issues/93553

    requires org.elasticsearch.cli;
    requires org.elasticsearch.base;
    requires org.elasticsearch.nativeaccess;
    requires org.elasticsearch.geo;
    requires org.elasticsearch.lz4;
    requires org.elasticsearch.pluginclassloader;
    requires org.elasticsearch.securesm;
    requires org.elasticsearch.xcontent;
    requires org.elasticsearch.logging;
    requires org.elasticsearch.plugin;
    requires org.elasticsearch.plugin.analysis;
    requires org.elasticsearch.grok;
    requires org.elasticsearch.tdigest;

    requires com.sun.jna;
    requires hppc;
    requires HdrHistogram;
    requires jopt.simple;
    requires log4j2.ecs.layout;
    requires org.lz4.java;

    requires org.apache.logging.log4j;
    requires org.apache.logging.log4j.core;

    requires org.apache.lucene.analysis.common;
    requires org.apache.lucene.backward_codecs;
    requires org.apache.lucene.core;
    requires org.apache.lucene.grouping;
    requires org.apache.lucene.highlighter;
    requires org.apache.lucene.join;
    requires org.apache.lucene.memory;
    requires org.apache.lucene.misc;
    requires org.apache.lucene.queries;
    requires org.apache.lucene.queryparser;
    requires org.apache.lucene.sandbox;
    requires org.apache.lucene.suggest;

    exports org.elasticsearch;
    exports org.elasticsearch.action;
    exports org.elasticsearch.action.admin.cluster.allocation;
    exports org.elasticsearch.action.admin.cluster.configuration;
    exports org.elasticsearch.action.admin.cluster.coordination;
    exports org.elasticsearch.action.admin.cluster.desirednodes;
    exports org.elasticsearch.action.admin.cluster.health;
    exports org.elasticsearch.action.admin.cluster.migration;
    exports org.elasticsearch.action.admin.cluster.node.hotthreads;
    exports org.elasticsearch.action.admin.cluster.node.info;
    exports org.elasticsearch.action.admin.cluster.node.reload;
    exports org.elasticsearch.action.admin.cluster.node.shutdown;
    exports org.elasticsearch.action.admin.cluster.node.stats;
    exports org.elasticsearch.action.admin.cluster.node.tasks.cancel;
    exports org.elasticsearch.action.admin.cluster.node.tasks.get;
    exports org.elasticsearch.action.admin.cluster.node.tasks.list;
    exports org.elasticsearch.action.admin.cluster.node.usage;
    exports org.elasticsearch.action.admin.cluster.remote;
    exports org.elasticsearch.action.admin.cluster.repositories.cleanup;
    exports org.elasticsearch.action.admin.cluster.repositories.delete;
    exports org.elasticsearch.action.admin.cluster.repositories.get;
    exports org.elasticsearch.action.admin.cluster.repositories.put;
    exports org.elasticsearch.action.admin.cluster.repositories.verify;
    exports org.elasticsearch.action.admin.cluster.reroute;
    exports org.elasticsearch.action.admin.cluster.settings;
    exports org.elasticsearch.action.admin.cluster.shards;
    exports org.elasticsearch.action.admin.cluster.snapshots.clone;
    exports org.elasticsearch.action.admin.cluster.snapshots.create;
    exports org.elasticsearch.action.admin.cluster.snapshots.delete;
    exports org.elasticsearch.action.admin.cluster.snapshots.features;
    exports org.elasticsearch.action.admin.cluster.snapshots.get;
    exports org.elasticsearch.action.admin.cluster.snapshots.get.shard;
    exports org.elasticsearch.action.admin.cluster.snapshots.restore;
    exports org.elasticsearch.action.admin.cluster.snapshots.status;
    exports org.elasticsearch.action.admin.cluster.state;
    exports org.elasticsearch.action.admin.cluster.stats;
    exports org.elasticsearch.action.admin.cluster.storedscripts;
    exports org.elasticsearch.action.admin.cluster.tasks;
    exports org.elasticsearch.action.admin.indices.alias;
    exports org.elasticsearch.action.admin.indices.alias.get;
    exports org.elasticsearch.action.admin.indices.analyze;
    exports org.elasticsearch.action.admin.indices.cache.clear;
    exports org.elasticsearch.action.admin.indices.close;
    exports org.elasticsearch.action.admin.indices.create;
    exports org.elasticsearch.action.admin.indices.dangling;
    exports org.elasticsearch.action.admin.indices.dangling.delete;
    exports org.elasticsearch.action.admin.indices.dangling.find;
    exports org.elasticsearch.action.admin.indices.dangling.import_index;
    exports org.elasticsearch.action.admin.indices.dangling.list;
    exports org.elasticsearch.action.admin.indices.delete;
    exports org.elasticsearch.action.admin.indices.diskusage;
    exports org.elasticsearch.action.admin.indices.flush;
    exports org.elasticsearch.action.admin.indices.forcemerge;
    exports org.elasticsearch.action.admin.indices.get;
    exports org.elasticsearch.action.admin.indices.mapping.get;
    exports org.elasticsearch.action.admin.indices.mapping.put;
    exports org.elasticsearch.action.admin.indices.open;
    exports org.elasticsearch.action.admin.indices.readonly;
    exports org.elasticsearch.action.admin.indices.recovery;
    exports org.elasticsearch.action.admin.indices.refresh;
    exports org.elasticsearch.action.admin.indices.resolve;
    exports org.elasticsearch.action.admin.indices.rollover;
    exports org.elasticsearch.action.admin.indices.segments;
    exports org.elasticsearch.action.admin.indices.settings.get;
    exports org.elasticsearch.action.admin.indices.settings.put;
    exports org.elasticsearch.action.admin.indices.shards;
    exports org.elasticsearch.action.admin.indices.shrink;
    exports org.elasticsearch.action.admin.indices.stats;
    exports org.elasticsearch.action.admin.indices.template.delete;
    exports org.elasticsearch.action.admin.indices.template.get;
    exports org.elasticsearch.action.admin.indices.template.post;
    exports org.elasticsearch.action.admin.indices.template.put;
    exports org.elasticsearch.action.admin.indices.validate.query;
    exports org.elasticsearch.action.bulk;
    exports org.elasticsearch.action.datastreams;
    exports org.elasticsearch.action.delete;
    exports org.elasticsearch.action.explain;
    exports org.elasticsearch.action.fieldcaps;
    exports org.elasticsearch.action.get;
    exports org.elasticsearch.action.index;
    exports org.elasticsearch.action.ingest;
    exports org.elasticsearch.action.resync;
    exports org.elasticsearch.action.search;
    exports org.elasticsearch.action.support;
    exports org.elasticsearch.action.support.broadcast;
    exports org.elasticsearch.action.support.broadcast.node;
    exports org.elasticsearch.action.support.broadcast.unpromotable;
    exports org.elasticsearch.action.support.master;
    exports org.elasticsearch.action.support.master.info;
    exports org.elasticsearch.action.support.nodes;
    exports org.elasticsearch.action.support.replication;
    exports org.elasticsearch.action.support.single.instance;
    exports org.elasticsearch.action.support.single.shard;
    exports org.elasticsearch.action.support.tasks;
    exports org.elasticsearch.action.termvectors;
    exports org.elasticsearch.action.update;
    exports org.elasticsearch.bootstrap;
    exports org.elasticsearch.client.internal;
    exports org.elasticsearch.client.internal.node;
    exports org.elasticsearch.client.internal.support;
    exports org.elasticsearch.client.internal.transport;
    exports org.elasticsearch.cluster;
    exports org.elasticsearch.cluster.ack;
    exports org.elasticsearch.cluster.action.index;
    exports org.elasticsearch.cluster.action.shard;
    exports org.elasticsearch.cluster.block;
    exports org.elasticsearch.cluster.coordination;
    exports org.elasticsearch.cluster.coordination.stateless;
    exports org.elasticsearch.cluster.health;
    exports org.elasticsearch.cluster.metadata;
    exports org.elasticsearch.cluster.node;
    exports org.elasticsearch.cluster.routing;
    exports org.elasticsearch.cluster.routing.allocation;
    exports org.elasticsearch.cluster.routing.allocation.allocator;
    exports org.elasticsearch.cluster.routing.allocation.command;
    exports org.elasticsearch.cluster.routing.allocation.decider;
    exports org.elasticsearch.cluster.service;
    exports org.elasticsearch.cluster.version;
    exports org.elasticsearch.common;
    exports org.elasticsearch.common.blobstore;
    exports org.elasticsearch.common.blobstore.fs;
    exports org.elasticsearch.common.blobstore.support;
    exports org.elasticsearch.common.breaker;
    exports org.elasticsearch.common.bytes;
    exports org.elasticsearch.common.cache;
    exports org.elasticsearch.common.cli;
    exports org.elasticsearch.common.collect;
    exports org.elasticsearch.common.component;
    exports org.elasticsearch.common.compress;
    exports org.elasticsearch.common.document;
    exports org.elasticsearch.common.file;
    exports org.elasticsearch.common.filesystem;
    exports org.elasticsearch.common.geo;
    exports org.elasticsearch.common.hash;
    exports org.elasticsearch.common.inject;
    exports org.elasticsearch.common.inject.binder;
    exports org.elasticsearch.common.inject.internal;
    exports org.elasticsearch.common.inject.matcher;
    exports org.elasticsearch.common.inject.multibindings;
    exports org.elasticsearch.common.inject.name;
    exports org.elasticsearch.common.inject.spi;
    exports org.elasticsearch.common.inject.util;
    exports org.elasticsearch.common.io;
    exports org.elasticsearch.common.io.stream;
    exports org.elasticsearch.common.logging;
    exports org.elasticsearch.common.lucene;
    exports org.elasticsearch.common.lucene.index;
    exports org.elasticsearch.common.lucene.search;
    exports org.elasticsearch.common.lucene.search.function;
    exports org.elasticsearch.common.lucene.store;
    exports org.elasticsearch.common.lucene.uid;
    exports org.elasticsearch.common.metrics;
    exports org.elasticsearch.common.network;
    exports org.elasticsearch.common.path;
    exports org.elasticsearch.common.recycler;
    exports org.elasticsearch.common.regex;
    exports org.elasticsearch.common.scheduler;
    exports org.elasticsearch.common.settings;
    exports org.elasticsearch.common.text;
    exports org.elasticsearch.common.time;
    exports org.elasticsearch.common.transport;
    exports org.elasticsearch.common.unit;
    exports org.elasticsearch.common.util;
    exports org.elasticsearch.common.util.concurrent;
    exports org.elasticsearch.common.util.iterable;
    exports org.elasticsearch.common.util.set;
    exports org.elasticsearch.common.xcontent;
    exports org.elasticsearch.common.xcontent.support;
    exports org.elasticsearch.discovery;
    exports org.elasticsearch.env;
    exports org.elasticsearch.features;
    exports org.elasticsearch.gateway;
    exports org.elasticsearch.health;
    exports org.elasticsearch.health.node;
    exports org.elasticsearch.health.node.tracker;
    exports org.elasticsearch.health.node.selection;
    exports org.elasticsearch.health.stats;
    exports org.elasticsearch.http;
    exports org.elasticsearch.index;
    exports org.elasticsearch.index.analysis;
    exports org.elasticsearch.index.bulk.stats;
    exports org.elasticsearch.index.cache;
    exports org.elasticsearch.index.cache.bitset;
    exports org.elasticsearch.index.cache.query;
    exports org.elasticsearch.index.cache.request;
    exports org.elasticsearch.index.codec;
    exports org.elasticsearch.index.codec.tsdb;
    exports org.elasticsearch.index.codec.bloomfilter;
    exports org.elasticsearch.index.engine;
    exports org.elasticsearch.index.fielddata;
    exports org.elasticsearch.index.fielddata.fieldcomparator;
    exports org.elasticsearch.index.fielddata.ordinals;
    exports org.elasticsearch.index.fielddata.plain;
    exports org.elasticsearch.index.fieldvisitor;
    exports org.elasticsearch.index.flush;
    exports org.elasticsearch.index.get;
    exports org.elasticsearch.index.mapper;
    exports org.elasticsearch.index.mapper.flattened;
    exports org.elasticsearch.index.mapper.vectors;
    exports org.elasticsearch.index.merge;
    exports org.elasticsearch.index.query;
    exports org.elasticsearch.index.query.functionscore;
    exports org.elasticsearch.index.query.support;
    exports org.elasticsearch.index.recovery;
    exports org.elasticsearch.index.refresh;
    exports org.elasticsearch.index.reindex;
    exports org.elasticsearch.index.search;
    exports org.elasticsearch.index.search.stats;
    exports org.elasticsearch.index.seqno;
    exports org.elasticsearch.index.shard;
    exports org.elasticsearch.index.similarity;
    exports org.elasticsearch.index.snapshots;
    exports org.elasticsearch.index.snapshots.blobstore;
    exports org.elasticsearch.index.stats;
    exports org.elasticsearch.index.store;
    exports org.elasticsearch.index.termvectors;
    exports org.elasticsearch.index.translog;
    exports org.elasticsearch.index.warmer;
    exports org.elasticsearch.indices;
    exports org.elasticsearch.indices.analysis;
    exports org.elasticsearch.indices.breaker;
    exports org.elasticsearch.indices.cluster;
    exports org.elasticsearch.indices.fielddata.cache;
    exports org.elasticsearch.indices.recovery;
    exports org.elasticsearch.indices.recovery.plan;
    exports org.elasticsearch.indices.store;
    exports org.elasticsearch.inference;
    exports org.elasticsearch.ingest;
    exports org.elasticsearch.internal
        to
            org.elasticsearch.serverless.version,
            org.elasticsearch.serverless.buildinfo,
            org.elasticsearch.serverless.constants;
    exports org.elasticsearch.lucene.analysis.miscellaneous;
    exports org.elasticsearch.lucene.grouping;
    exports org.elasticsearch.lucene.queries;
    exports org.elasticsearch.lucene.search.uhighlight;
    exports org.elasticsearch.lucene.search.vectorhighlight;
    exports org.elasticsearch.lucene.similarity;
    exports org.elasticsearch.lucene.util;
    exports org.elasticsearch.monitor;
    exports org.elasticsearch.monitor.fs;
    exports org.elasticsearch.monitor.jvm;
    exports org.elasticsearch.monitor.os;
    exports org.elasticsearch.monitor.process;
    exports org.elasticsearch.node;
    exports org.elasticsearch.node.internal to org.elasticsearch.internal.sigterm;
    exports org.elasticsearch.persistent;
    exports org.elasticsearch.persistent.decider;
    exports org.elasticsearch.plugins;
    exports org.elasticsearch.plugins.interceptor to org.elasticsearch.security, org.elasticsearch.serverless.rest;
    exports org.elasticsearch.plugins.spi;
    exports org.elasticsearch.repositories;
    exports org.elasticsearch.repositories.blobstore;
    exports org.elasticsearch.repositories.fs;
    exports org.elasticsearch.reservedstate;
    exports org.elasticsearch.rest;
    exports org.elasticsearch.rest.action;
    exports org.elasticsearch.rest.action.admin.cluster;
    exports org.elasticsearch.rest.action.admin.cluster.dangling;
    exports org.elasticsearch.rest.action.admin.indices;
    exports org.elasticsearch.rest.action.cat;
    exports org.elasticsearch.rest.action.document;
    exports org.elasticsearch.rest.action.ingest;
    exports org.elasticsearch.rest.action.search;
    exports org.elasticsearch.script;
    exports org.elasticsearch.script.field;
    exports org.elasticsearch.script.field.vectors;
    exports org.elasticsearch.search;
    exports org.elasticsearch.search.aggregations;
    exports org.elasticsearch.search.aggregations.bucket;
    exports org.elasticsearch.search.aggregations.bucket.composite;
    exports org.elasticsearch.search.aggregations.bucket.countedterms;
    exports org.elasticsearch.search.aggregations.bucket.filter;
    exports org.elasticsearch.search.aggregations.bucket.geogrid;
    exports org.elasticsearch.search.aggregations.bucket.global;
    exports org.elasticsearch.search.aggregations.bucket.histogram;
    exports org.elasticsearch.search.aggregations.bucket.missing;
    exports org.elasticsearch.search.aggregations.bucket.nested;
    exports org.elasticsearch.search.aggregations.bucket.range;
    exports org.elasticsearch.search.aggregations.bucket.sampler;
    exports org.elasticsearch.search.aggregations.bucket.sampler.random;
    exports org.elasticsearch.search.aggregations.bucket.terms;
    exports org.elasticsearch.search.aggregations.bucket.terms.heuristic;
    exports org.elasticsearch.search.aggregations.metrics;
    exports org.elasticsearch.search.aggregations.pipeline;
    exports org.elasticsearch.search.aggregations.support;
    exports org.elasticsearch.search.aggregations.support.values;
    exports org.elasticsearch.search.builder;
    exports org.elasticsearch.search.collapse;
    exports org.elasticsearch.search.dfs;
    exports org.elasticsearch.search.fetch;
    exports org.elasticsearch.search.fetch.subphase;
    exports org.elasticsearch.search.fetch.subphase.highlight;
    exports org.elasticsearch.search.internal;
    exports org.elasticsearch.search.lookup;
    exports org.elasticsearch.search.profile;
    exports org.elasticsearch.search.profile.aggregation;
    exports org.elasticsearch.search.profile.dfs;
    exports org.elasticsearch.search.profile.query;
    exports org.elasticsearch.search.query;
    exports org.elasticsearch.search.rank;
    exports org.elasticsearch.search.rescore;
    exports org.elasticsearch.search.retriever;
    exports org.elasticsearch.search.runtime;
    exports org.elasticsearch.search.searchafter;
    exports org.elasticsearch.search.slice;
    exports org.elasticsearch.search.sort;
    exports org.elasticsearch.search.suggest;
    exports org.elasticsearch.search.suggest.completion;
    exports org.elasticsearch.search.suggest.completion.context;
    exports org.elasticsearch.search.suggest.phrase;
    exports org.elasticsearch.search.suggest.term;
    exports org.elasticsearch.search.vectors;
    exports org.elasticsearch.shutdown;
    exports org.elasticsearch.snapshots;
    exports org.elasticsearch.synonyms;
    exports org.elasticsearch.tasks;
    exports org.elasticsearch.threadpool;
    exports org.elasticsearch.transport;
    exports org.elasticsearch.upgrades;
    exports org.elasticsearch.usage;
    exports org.elasticsearch.watcher;

    opens org.elasticsearch.common.logging to org.apache.logging.log4j.core;

    exports org.elasticsearch.action.datastreams.lifecycle;
    exports org.elasticsearch.action.datastreams.autosharding;
    exports org.elasticsearch.action.downsample;
    exports org.elasticsearch.plugins.internal
        to
            org.elasticsearch.metering,
            org.elasticsearch.settings.secure,
            org.elasticsearch.serverless.constants,
            org.elasticsearch.serverless.apifiltering;
    exports org.elasticsearch.telemetry.tracing;
    exports org.elasticsearch.telemetry;
    exports org.elasticsearch.telemetry.metric;

    provides java.util.spi.CalendarDataProvider with org.elasticsearch.common.time.IsoCalendarDataProvider;
    provides org.elasticsearch.xcontent.ErrorOnUnknown with org.elasticsearch.common.xcontent.SuggestingErrorOnUnknown;
    provides org.elasticsearch.xcontent.XContentBuilderExtension with org.elasticsearch.common.xcontent.XContentElasticsearchExtension;
    provides org.elasticsearch.cli.CliToolProvider
        with
            org.elasticsearch.cluster.coordination.NodeToolCliProvider,
            org.elasticsearch.index.shard.ShardToolCliProvider;

    uses org.elasticsearch.reservedstate.ReservedClusterStateHandlerProvider;
    uses org.elasticsearch.jdk.ModuleQualifiedExportsService;
    uses org.elasticsearch.node.internal.TerminationHandlerProvider;
    uses org.elasticsearch.internal.VersionExtension;
    uses org.elasticsearch.internal.BuildExtension;
    uses org.elasticsearch.features.FeatureSpecification;

    provides org.elasticsearch.features.FeatureSpecification
        with
            org.elasticsearch.features.FeatureInfrastructureFeatures,
            org.elasticsearch.health.HealthFeatures,
            org.elasticsearch.cluster.service.TransportFeatures,
            org.elasticsearch.cluster.metadata.MetadataFeatures,
            org.elasticsearch.rest.RestFeatures,
            org.elasticsearch.indices.IndicesFeatures,
            org.elasticsearch.search.retriever.RetrieversFeatures;

    uses org.elasticsearch.plugins.internal.SettingsExtension;
    uses RestExtension;
    uses org.elasticsearch.action.admin.cluster.node.info.ComponentVersionNumber;

    provides org.apache.lucene.codecs.PostingsFormat
        with
            org.elasticsearch.index.codec.bloomfilter.ES85BloomFilterPostingsFormat,
            org.elasticsearch.index.codec.bloomfilter.ES87BloomFilterPostingsFormat,
            org.elasticsearch.index.codec.postings.ES812PostingsFormat;
    provides org.apache.lucene.codecs.DocValuesFormat with ES87TSDBDocValuesFormat;
    provides org.apache.lucene.codecs.KnnVectorsFormat
        with
            org.elasticsearch.index.codec.vectors.ES813FlatVectorFormat,
            org.elasticsearch.index.codec.vectors.ES813Int8FlatVectorFormat;

    exports org.elasticsearch.cluster.routing.allocation.shards
        to
            org.elasticsearch.shardhealth,
            org.elasticsearch.serverless.shardhealth,
            org.elasticsearch.serverless.apifiltering;
    exports org.elasticsearch.lucene.spatial;
}
