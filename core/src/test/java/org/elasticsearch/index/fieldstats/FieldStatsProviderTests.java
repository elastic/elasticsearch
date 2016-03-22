/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.fieldstats;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.BaseDirectoryWrapper;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.lucene.index.ElasticsearchDirectoryReader;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.AnalysisRegistry;
import org.elasticsearch.index.analysis.AnalysisService;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.Engine.Searcher;
import org.elasticsearch.index.fieldstats.FieldStatsProvider.Relation;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MapperService.MergeReason;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.similarity.SimilarityService;
import org.elasticsearch.indices.IndicesModule;
import org.elasticsearch.indices.mapper.MapperRegistry;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.IndexSettingsModule;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.chrono.ISOChronology;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.Collections;

import static org.elasticsearch.common.settings.Settings.settingsBuilder;
import static org.hamcrest.Matchers.equalTo;

public class FieldStatsProviderTests extends ESTestCase {

    private DirectoryReader directoryReader;
    private Searcher searcher;
    private FieldStatsProvider fieldStatsProvider;
    private BaseDirectoryWrapper dir;
    private AnalysisRegistry analysisRegistry;

    @Before
    public void setup() throws IOException {
        Settings nodeSettings = settingsBuilder().put(Environment.PATH_HOME_SETTING.getKey(), createTempDir()).build();
        IndexSettings settings = IndexSettingsModule.newIndexSettings("_na", nodeSettings);
        SimilarityService similarityService = new SimilarityService(settings, Collections.emptyMap());
        analysisRegistry = new AnalysisRegistry(null, new Environment(nodeSettings));
        AnalysisService analysisService = analysisRegistry.build(settings);
        IndicesModule indicesModule = new IndicesModule();
        MapperRegistry mapperRegistry = indicesModule.getMapperRegistry();
        MapperService service = new MapperService(settings, analysisService, similarityService, mapperRegistry, () -> null);
        putMapping(service);
        dir = newDirectory();
        IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());
        indexDocument(service, w, "1", 50L, 50.2f, 50.2, "cherry", new DateTime(2014, 1, 1, 0, 0, 0, ISOChronology.getInstanceUTC()),
                "10.10.0.10");
        indexDocument(service, w, "2", 60L, 60.1f, 60.1, "damson", new DateTime(2014, 2, 1, 0, 0, 0, ISOChronology.getInstanceUTC()),
                "10.10.0.20");
        indexDocument(service, w, "3", 70L, 70.6f, 70.6, "grape", new DateTime(2014, 3, 1, 0, 0, 0, ISOChronology.getInstanceUTC()),
                "10.10.0.30");
        indexDocument(service, w, "4", 80L, 80.2f, 80.2, "kiwi", new DateTime(2014, 4, 1, 0, 0, 0, ISOChronology.getInstanceUTC()),
                "10.10.0.40");
        indexDocument(service, w, "5", 90L, 90.4f, 90.4, "lemon", new DateTime(2014, 5, 1, 0, 0, 0, ISOChronology.getInstanceUTC()),
                "10.10.0.50");
        indexDocument(service, w, "6", 100L, 100.3f, 100.3, "orange", new DateTime(2014, 6, 1, 0, 0, 0, ISOChronology.getInstanceUTC()),
                "10.10.0.60");
        directoryReader = DirectoryReader.open(w, true, true);
        w.close();
        ShardId shard = new ShardId("index", "_na_", 0);
        directoryReader = ElasticsearchDirectoryReader.wrap(directoryReader, shard);
        IndexSearcher s = new IndexSearcher(directoryReader);
        searcher = new Engine.Searcher("test", s);
        fieldStatsProvider = new FieldStatsProvider(searcher, service);
    }

    @After
    public void teardown() throws IOException {
        searcher.close();
        directoryReader.close();
        dir.close();
        analysisRegistry.close();
    }

    public void testiIsFieldWithinQueryLong() throws IOException {
        assertThat(fieldStatsProvider.isFieldWithinQuery("long_field", 10L, 200L, true, true, DateTimeZone.UTC, null),
                equalTo(Relation.WITHIN));
        assertThat(fieldStatsProvider.isFieldWithinQuery("long_field", 10L, null, true, true, DateTimeZone.UTC, null),
                equalTo(Relation.WITHIN));
        assertThat(fieldStatsProvider.isFieldWithinQuery("long_field", null, 200L, true, true, DateTimeZone.UTC, null),
                equalTo(Relation.WITHIN));
        assertThat(fieldStatsProvider.isFieldWithinQuery("long_field", null, null, true, true, DateTimeZone.UTC, null),
                equalTo(Relation.WITHIN));
        assertThat(fieldStatsProvider.isFieldWithinQuery("long_field", 10L, 100L, true, true, DateTimeZone.UTC, null),
                equalTo(Relation.WITHIN));
        assertThat(fieldStatsProvider.isFieldWithinQuery("long_field", 50L, 200L, true, true, DateTimeZone.UTC, null),
                equalTo(Relation.WITHIN));
        assertThat(fieldStatsProvider.isFieldWithinQuery("long_field", 30L, 80L, true, true, DateTimeZone.UTC, null),
                equalTo(Relation.INTERSECTS));
        assertThat(fieldStatsProvider.isFieldWithinQuery("long_field", 80L, 200L, true, true, DateTimeZone.UTC, null),
                equalTo(Relation.INTERSECTS));
        assertThat(fieldStatsProvider.isFieldWithinQuery("long_field", 60L, 80L, true, true, DateTimeZone.UTC, null),
                equalTo(Relation.INTERSECTS));
        assertThat(fieldStatsProvider.isFieldWithinQuery("long_field", 10L, 100L, true, false, DateTimeZone.UTC, null),
                equalTo(Relation.INTERSECTS));
        assertThat(fieldStatsProvider.isFieldWithinQuery("long_field", 50L, 200L, false, true, DateTimeZone.UTC, null),
                equalTo(Relation.INTERSECTS));
        assertThat(fieldStatsProvider.isFieldWithinQuery("long_field", 100L, 200L, true, true, DateTimeZone.UTC, null),
                equalTo(Relation.INTERSECTS));
        assertThat(fieldStatsProvider.isFieldWithinQuery("long_field", 1L, 50L, true, true, DateTimeZone.UTC, null),
                equalTo(Relation.INTERSECTS));
        assertThat(fieldStatsProvider.isFieldWithinQuery("long_field", 150L, 200L, true, true, DateTimeZone.UTC, null),
                equalTo(Relation.DISJOINT));
        assertThat(fieldStatsProvider.isFieldWithinQuery("long_field", 1L, 8L, true, true, DateTimeZone.UTC, null),
                equalTo(Relation.DISJOINT));
        assertThat(fieldStatsProvider.isFieldWithinQuery("long_field", null, 8L, true, true, DateTimeZone.UTC, null),
                equalTo(Relation.DISJOINT));
        assertThat(fieldStatsProvider.isFieldWithinQuery("long_field", 150L, null, true, true, DateTimeZone.UTC, null),
                equalTo(Relation.DISJOINT));
        assertThat(fieldStatsProvider.isFieldWithinQuery("long_field", 100L, 200L, false, true, DateTimeZone.UTC, null),
                equalTo(Relation.DISJOINT));
        assertThat(fieldStatsProvider.isFieldWithinQuery("long_field", 1L, 50L, true, false, DateTimeZone.UTC, null),
                equalTo(Relation.DISJOINT));
    }

    public void testiIsFieldWithinQueryFloat() throws IOException {
        assertThat(fieldStatsProvider.isFieldWithinQuery("float_field", 10.8f, 200.5f, true, true, DateTimeZone.UTC, null),
                equalTo(Relation.WITHIN));
        assertThat(fieldStatsProvider.isFieldWithinQuery("float_field", 10.8f, null, true, true, DateTimeZone.UTC, null),
                equalTo(Relation.WITHIN));
        assertThat(fieldStatsProvider.isFieldWithinQuery("float_field", null, 200.5f, true, true, DateTimeZone.UTC, null),
                equalTo(Relation.WITHIN));
        assertThat(fieldStatsProvider.isFieldWithinQuery("float_field", null, null, true, true, DateTimeZone.UTC, null),
                equalTo(Relation.WITHIN));
        assertThat(fieldStatsProvider.isFieldWithinQuery("float_field", 10.8f, 100.3f, true, true, DateTimeZone.UTC, null),
                equalTo(Relation.WITHIN));
        assertThat(fieldStatsProvider.isFieldWithinQuery("float_field", 50.2f, 200.5f, true, true, DateTimeZone.UTC, null),
                equalTo(Relation.WITHIN));
        assertThat(fieldStatsProvider.isFieldWithinQuery("float_field", 30.5f, 80.1f, true, true, DateTimeZone.UTC, null),
                equalTo(Relation.INTERSECTS));
        assertThat(fieldStatsProvider.isFieldWithinQuery("float_field", 80.1f, 200.5f, true, true, DateTimeZone.UTC, null),
                equalTo(Relation.INTERSECTS));
        assertThat(fieldStatsProvider.isFieldWithinQuery("float_field", 10.8f, 100.3f, true, false, DateTimeZone.UTC, null),
                equalTo(Relation.INTERSECTS));
        assertThat(fieldStatsProvider.isFieldWithinQuery("float_field", 50.2f, 200.5f, false, true, DateTimeZone.UTC, null),
                equalTo(Relation.INTERSECTS));
        assertThat(fieldStatsProvider.isFieldWithinQuery("float_field", 100.3f, 200.5f, true, true, DateTimeZone.UTC, null),
                equalTo(Relation.INTERSECTS));
        assertThat(fieldStatsProvider.isFieldWithinQuery("float_field", 1.9f, 50.2f, true, true, DateTimeZone.UTC, null),
                equalTo(Relation.INTERSECTS));
        assertThat(fieldStatsProvider.isFieldWithinQuery("float_field", 60.9f, 80.1f, true, true, DateTimeZone.UTC, null),
                equalTo(Relation.INTERSECTS));
        assertThat(fieldStatsProvider.isFieldWithinQuery("float_field", 150.4f, 200.5f, true, true, DateTimeZone.UTC, null),
                equalTo(Relation.DISJOINT));
        assertThat(fieldStatsProvider.isFieldWithinQuery("float_field", 1.9f, 8.1f, true, true, DateTimeZone.UTC, null),
                equalTo(Relation.DISJOINT));
        assertThat(fieldStatsProvider.isFieldWithinQuery("float_field", null, 8.1f, true, true, DateTimeZone.UTC, null),
                equalTo(Relation.DISJOINT));
        assertThat(fieldStatsProvider.isFieldWithinQuery("float_field", 150.4f, null, true, true, DateTimeZone.UTC, null),
                equalTo(Relation.DISJOINT));
        assertThat(fieldStatsProvider.isFieldWithinQuery("float_field", 100.3f, 200.5f, false, true, DateTimeZone.UTC, null),
                equalTo(Relation.DISJOINT));
        assertThat(fieldStatsProvider.isFieldWithinQuery("float_field", 1.9f, 50.2f, true, false, DateTimeZone.UTC, null),
                equalTo(Relation.DISJOINT));
    }

    public void testiIsFieldWithinQueryDouble() throws IOException {
        assertThat(fieldStatsProvider.isFieldWithinQuery("double_field", 10.8, 200.5, true, true, DateTimeZone.UTC, null),
                equalTo(Relation.WITHIN));
        assertThat(fieldStatsProvider.isFieldWithinQuery("double_field", 10.8, null, true, true, DateTimeZone.UTC, null),
                equalTo(Relation.WITHIN));
        assertThat(fieldStatsProvider.isFieldWithinQuery("double_field", null, 200.5, true, true, DateTimeZone.UTC, null),
                equalTo(Relation.WITHIN));
        assertThat(fieldStatsProvider.isFieldWithinQuery("double_field", null, null, true, true, DateTimeZone.UTC, null),
                equalTo(Relation.WITHIN));
        assertThat(fieldStatsProvider.isFieldWithinQuery("double_field", 10.8, 100.3, true, true, DateTimeZone.UTC, null),
                equalTo(Relation.WITHIN));
        assertThat(fieldStatsProvider.isFieldWithinQuery("double_field", 50.2, 200.5, true, true, DateTimeZone.UTC, null),
                equalTo(Relation.WITHIN));
        assertThat(fieldStatsProvider.isFieldWithinQuery("double_field", 30.5, 80.1, true, true, DateTimeZone.UTC, null),
                equalTo(Relation.INTERSECTS));
        assertThat(fieldStatsProvider.isFieldWithinQuery("double_field", 80.1, 200.5, true, true, DateTimeZone.UTC, null),
                equalTo(Relation.INTERSECTS));
        assertThat(fieldStatsProvider.isFieldWithinQuery("double_field", 60.9, 80.1, true, true, DateTimeZone.UTC, null),
                equalTo(Relation.INTERSECTS));
        assertThat(fieldStatsProvider.isFieldWithinQuery("double_field", 10.8, 100.3, true, false, DateTimeZone.UTC, null),
                equalTo(Relation.INTERSECTS));
        assertThat(fieldStatsProvider.isFieldWithinQuery("double_field", 50.2, 200.5, false, true, DateTimeZone.UTC, null),
                equalTo(Relation.INTERSECTS));
        assertThat(fieldStatsProvider.isFieldWithinQuery("double_field", 100.3, 200.5, true, true, DateTimeZone.UTC, null),
                equalTo(Relation.INTERSECTS));
        assertThat(fieldStatsProvider.isFieldWithinQuery("double_field", 1.9, 50.2, true, true, DateTimeZone.UTC, null),
                equalTo(Relation.INTERSECTS));
        assertThat(fieldStatsProvider.isFieldWithinQuery("double_field", 150.4, 200.5, true, true, DateTimeZone.UTC, null),
                equalTo(Relation.DISJOINT));
        assertThat(fieldStatsProvider.isFieldWithinQuery("double_field", 1.9, 8.1, true, true, DateTimeZone.UTC, null),
                equalTo(Relation.DISJOINT));
        assertThat(fieldStatsProvider.isFieldWithinQuery("double_field", null, 8.1, true, true, DateTimeZone.UTC, null),
                equalTo(Relation.DISJOINT));
        assertThat(fieldStatsProvider.isFieldWithinQuery("double_field", 150.4, null, true, true, DateTimeZone.UTC, null),
                equalTo(Relation.DISJOINT));
        assertThat(fieldStatsProvider.isFieldWithinQuery("long_field", 100.3, 200.5, false, true, DateTimeZone.UTC, null),
                equalTo(Relation.DISJOINT));
        assertThat(fieldStatsProvider.isFieldWithinQuery("long_field", 1.9, 50.2, true, false, DateTimeZone.UTC, null),
                equalTo(Relation.DISJOINT));
    }

    public void testiIsFieldWithinQueryText() throws IOException {
        assertThat(fieldStatsProvider.isFieldWithinQuery("text_field", new BytesRef("banana"), new BytesRef("zebra"), true, true,
                DateTimeZone.UTC, null), equalTo(Relation.WITHIN));
        assertThat(fieldStatsProvider.isFieldWithinQuery("text_field", new BytesRef("banana"), null, true, true, DateTimeZone.UTC, null),
                equalTo(Relation.WITHIN));
        assertThat(fieldStatsProvider.isFieldWithinQuery("text_field", null, new BytesRef("zebra"), true, true, DateTimeZone.UTC, null),
                equalTo(Relation.WITHIN));
        assertThat(fieldStatsProvider.isFieldWithinQuery("text_field", null, null, true, true, DateTimeZone.UTC, null),
                equalTo(Relation.WITHIN));
        assertThat(fieldStatsProvider.isFieldWithinQuery("text_field", new BytesRef("banana"), new BytesRef("orange"), true, true,
                DateTimeZone.UTC, null), equalTo(Relation.WITHIN));
        assertThat(fieldStatsProvider.isFieldWithinQuery("text_field", new BytesRef("cherry"), new BytesRef("zebra"), true, true,
                DateTimeZone.UTC, null), equalTo(Relation.WITHIN));
        assertThat(fieldStatsProvider.isFieldWithinQuery("text_field", new BytesRef("banana"), new BytesRef("grape"), true, true,
                DateTimeZone.UTC, null), equalTo(Relation.INTERSECTS));
        assertThat(fieldStatsProvider.isFieldWithinQuery("text_field", new BytesRef("grape"), new BytesRef("zebra"), true, true,
                DateTimeZone.UTC, null), equalTo(Relation.INTERSECTS));
        assertThat(fieldStatsProvider.isFieldWithinQuery("text_field", new BytesRef("lime"), new BytesRef("mango"), true, true,
                DateTimeZone.UTC, null), equalTo(Relation.INTERSECTS));
        assertThat(fieldStatsProvider.isFieldWithinQuery("text_field", new BytesRef("banana"), new BytesRef("orange"), true, false,
                DateTimeZone.UTC, null), equalTo(Relation.INTERSECTS));
        assertThat(fieldStatsProvider.isFieldWithinQuery("text_field", new BytesRef("cherry"), new BytesRef("zebra"), false, true,
                DateTimeZone.UTC, null), equalTo(Relation.INTERSECTS));
        assertThat(fieldStatsProvider.isFieldWithinQuery("text_field", new BytesRef("orange"), new BytesRef("zebra"), true, true,
                DateTimeZone.UTC, null), equalTo(Relation.INTERSECTS));
        assertThat(fieldStatsProvider.isFieldWithinQuery("text_field", new BytesRef("apple"), new BytesRef("cherry"), true, true,
                DateTimeZone.UTC, null), equalTo(Relation.INTERSECTS));
        assertThat(fieldStatsProvider.isFieldWithinQuery("text_field", new BytesRef("peach"), new BytesRef("zebra"), true, true,
                DateTimeZone.UTC, null), equalTo(Relation.DISJOINT));
        assertThat(fieldStatsProvider.isFieldWithinQuery("text_field", new BytesRef("apple"), new BytesRef("banana"), true, true,
                DateTimeZone.UTC, null), equalTo(Relation.DISJOINT));
        assertThat(fieldStatsProvider.isFieldWithinQuery("text_field", null, new BytesRef("banana"), true, true, DateTimeZone.UTC, null),
                equalTo(Relation.DISJOINT));
        assertThat(fieldStatsProvider.isFieldWithinQuery("text_field", new BytesRef("peach"), null, true, true, DateTimeZone.UTC, null),
                equalTo(Relation.DISJOINT));
        assertThat(fieldStatsProvider.isFieldWithinQuery("text_field", new BytesRef("orange"), new BytesRef("zebra"), false, true,
                DateTimeZone.UTC, null), equalTo(Relation.DISJOINT));
        assertThat(fieldStatsProvider.isFieldWithinQuery("text_field", new BytesRef("apple"), new BytesRef("cherry"), true, false,
                DateTimeZone.UTC, null), equalTo(Relation.DISJOINT));
    }

    public void testiIsFieldWithinQueryKeyword() throws IOException {
        assertThat(fieldStatsProvider.isFieldWithinQuery("keyword_field", new BytesRef("banana"), new BytesRef("zebra"), true, true,
                DateTimeZone.UTC, null), equalTo(Relation.WITHIN));
        assertThat(fieldStatsProvider.isFieldWithinQuery("keyword_field", new BytesRef("banana"), null, true, true, DateTimeZone.UTC, null),
                equalTo(Relation.WITHIN));
        assertThat(fieldStatsProvider.isFieldWithinQuery("keyword_field", null, new BytesRef("zebra"), true, true, DateTimeZone.UTC, null),
                equalTo(Relation.WITHIN));
        assertThat(fieldStatsProvider.isFieldWithinQuery("keyword_field", null, null, true, true, DateTimeZone.UTC, null),
                equalTo(Relation.WITHIN));
        assertThat(fieldStatsProvider.isFieldWithinQuery("keyword_field", new BytesRef("banana"), new BytesRef("orange"), true, true,
                DateTimeZone.UTC, null), equalTo(Relation.WITHIN));
        assertThat(fieldStatsProvider.isFieldWithinQuery("keyword_field", new BytesRef("cherry"), new BytesRef("zebra"), true, true,
                DateTimeZone.UTC, null), equalTo(Relation.WITHIN));
        assertThat(fieldStatsProvider.isFieldWithinQuery("keyword_field", new BytesRef("banana"), new BytesRef("grape"), true, true,
                DateTimeZone.UTC, null), equalTo(Relation.INTERSECTS));
        assertThat(fieldStatsProvider.isFieldWithinQuery("keyword_field", new BytesRef("grape"), new BytesRef("zebra"), true, true,
                DateTimeZone.UTC, null), equalTo(Relation.INTERSECTS));
        assertThat(fieldStatsProvider.isFieldWithinQuery("keyword_field", new BytesRef("lime"), new BytesRef("mango"), true, true,
                DateTimeZone.UTC, null), equalTo(Relation.INTERSECTS));
        assertThat(fieldStatsProvider.isFieldWithinQuery("keyword_field", new BytesRef("banana"), new BytesRef("orange"), true, false,
                DateTimeZone.UTC, null), equalTo(Relation.INTERSECTS));
        assertThat(fieldStatsProvider.isFieldWithinQuery("keyword_field", new BytesRef("cherry"), new BytesRef("zebra"), false, true,
                DateTimeZone.UTC, null), equalTo(Relation.INTERSECTS));
        assertThat(fieldStatsProvider.isFieldWithinQuery("keyword_field", new BytesRef("orange"), new BytesRef("zebra"), true, true,
                DateTimeZone.UTC, null), equalTo(Relation.INTERSECTS));
        assertThat(fieldStatsProvider.isFieldWithinQuery("keyword_field", new BytesRef("apple"), new BytesRef("cherry"), true, true,
                DateTimeZone.UTC, null), equalTo(Relation.INTERSECTS));
        assertThat(fieldStatsProvider.isFieldWithinQuery("keyword_field", new BytesRef("peach"), new BytesRef("zebra"), true, true,
                DateTimeZone.UTC, null), equalTo(Relation.DISJOINT));
        assertThat(fieldStatsProvider.isFieldWithinQuery("keyword_field", new BytesRef("apple"), new BytesRef("banana"), true, true,
                DateTimeZone.UTC, null), equalTo(Relation.DISJOINT));
        assertThat(fieldStatsProvider.isFieldWithinQuery("keyword_field", null, new BytesRef("banana"), true, true, DateTimeZone.UTC, null),
                equalTo(Relation.DISJOINT));
        assertThat(fieldStatsProvider.isFieldWithinQuery("keyword_field", new BytesRef("peach"), null, true, true, DateTimeZone.UTC, null),
                equalTo(Relation.DISJOINT));
        assertThat(fieldStatsProvider.isFieldWithinQuery("keyword_field", new BytesRef("orange"), new BytesRef("zebra"), false, true,
                DateTimeZone.UTC, null), equalTo(Relation.DISJOINT));
        assertThat(fieldStatsProvider.isFieldWithinQuery("keyword_field", new BytesRef("apple"), new BytesRef("cherry"), true, false,
                DateTimeZone.UTC, null), equalTo(Relation.DISJOINT));
    }

    public void testiIsFieldWithinQueryDate() throws IOException {
        assertThat(fieldStatsProvider.isFieldWithinQuery("date_field", "2013-01-01", "now", true, true, DateTimeZone.UTC, null),
                equalTo(Relation.WITHIN));
        assertThat(fieldStatsProvider.isFieldWithinQuery("date_field", "2013-01-01", null, true, true, DateTimeZone.UTC, null),
                equalTo(Relation.WITHIN));
        assertThat(fieldStatsProvider.isFieldWithinQuery("date_field", null, "now", true, true, DateTimeZone.UTC, null),
                equalTo(Relation.WITHIN));
        assertThat(fieldStatsProvider.isFieldWithinQuery("date_field", null, null, true, true, DateTimeZone.UTC, null),
                equalTo(Relation.WITHIN));
        assertThat(fieldStatsProvider.isFieldWithinQuery("date_field", "2013-01-01", "2014-06-01", true, true, DateTimeZone.UTC, null),
                equalTo(Relation.WITHIN));
        assertThat(fieldStatsProvider.isFieldWithinQuery("date_field", "2014-01-01", "now", true, true, DateTimeZone.UTC, null),
                equalTo(Relation.WITHIN));
        assertThat(fieldStatsProvider.isFieldWithinQuery("date_field", "2013-01-01", "2014-03-01", true, true, DateTimeZone.UTC, null),
                equalTo(Relation.INTERSECTS));
        assertThat(fieldStatsProvider.isFieldWithinQuery("date_field", "2014-03-01", "now", true, true, DateTimeZone.UTC, null),
                equalTo(Relation.INTERSECTS));
        assertThat(fieldStatsProvider.isFieldWithinQuery("date_field", "2014-03-01", "2014-05-01", true, true, DateTimeZone.UTC, null),
                equalTo(Relation.INTERSECTS));
        assertThat(fieldStatsProvider.isFieldWithinQuery("date_field", "2013-01-01", "2014-06-01", true, false, DateTimeZone.UTC, null),
                equalTo(Relation.INTERSECTS));
        assertThat(fieldStatsProvider.isFieldWithinQuery("date_field", "2014-01-01", "now", false, true, DateTimeZone.UTC, null),
                equalTo(Relation.INTERSECTS));
        assertThat(fieldStatsProvider.isFieldWithinQuery("date_field", "2014-06-01", "now", true, true, DateTimeZone.UTC, null),
                equalTo(Relation.INTERSECTS));
        assertThat(fieldStatsProvider.isFieldWithinQuery("date_field", "2013-01-01", "2014-01-01", true, true, DateTimeZone.UTC, null),
                equalTo(Relation.INTERSECTS));
        assertThat(fieldStatsProvider.isFieldWithinQuery("date_field", "2015-01-01", "now", true, true, DateTimeZone.UTC, null),
                equalTo(Relation.DISJOINT));
        assertThat(fieldStatsProvider.isFieldWithinQuery("date_field", "2013-01-01", "2013-09-01", true, true, DateTimeZone.UTC, null),
                equalTo(Relation.DISJOINT));
        assertThat(fieldStatsProvider.isFieldWithinQuery("date_field", null, "2013-09-01", true, true, DateTimeZone.UTC, null),
                equalTo(Relation.DISJOINT));
        assertThat(fieldStatsProvider.isFieldWithinQuery("date_field", "2015-01-01", null, true, true, DateTimeZone.UTC, null),
                equalTo(Relation.DISJOINT));
        assertThat(fieldStatsProvider.isFieldWithinQuery("date_field", "2014-06-01", "now", false, true, DateTimeZone.UTC, null),
                equalTo(Relation.DISJOINT));
        assertThat(fieldStatsProvider.isFieldWithinQuery("date_field", "2013-01-01", "2014-01-01", true, false, DateTimeZone.UTC, null),
                equalTo(Relation.DISJOINT));
    }

    public void testiIsFieldWithinQueryIp() throws IOException {
        assertThat(fieldStatsProvider.isFieldWithinQuery("ip_field", "10.10.0.1", "10.20.0.1", true, true, DateTimeZone.UTC, null),
                equalTo(Relation.WITHIN));
        assertThat(fieldStatsProvider.isFieldWithinQuery("ip_field", "10.10.0.1", null, true, true, DateTimeZone.UTC, null),
                equalTo(Relation.WITHIN));
        assertThat(fieldStatsProvider.isFieldWithinQuery("ip_field", null, "10.20.0.1", true, true, DateTimeZone.UTC, null),
                equalTo(Relation.WITHIN));
        assertThat(fieldStatsProvider.isFieldWithinQuery("ip_field", null, null, true, true, DateTimeZone.UTC, null),
                equalTo(Relation.WITHIN));
        assertThat(fieldStatsProvider.isFieldWithinQuery("ip_field", "10.10.0.1", "10.10.0.60", true, true, DateTimeZone.UTC, null),
                equalTo(Relation.WITHIN));
        assertThat(fieldStatsProvider.isFieldWithinQuery("ip_field", "10.10.0.10", "10.20.0.1", true, true, DateTimeZone.UTC, null),
                equalTo(Relation.WITHIN));
        assertThat(fieldStatsProvider.isFieldWithinQuery("ip_field", "10.10.0.1", "10.10.0.40", true, true, DateTimeZone.UTC, null),
                equalTo(Relation.INTERSECTS));
        assertThat(fieldStatsProvider.isFieldWithinQuery("ip_field", "10.10.0.40", "10.20.0.1", true, true, DateTimeZone.UTC, null),
                equalTo(Relation.INTERSECTS));
        assertThat(fieldStatsProvider.isFieldWithinQuery("ip_field", "10.10.0.30", "10.10.0.40", true, true, DateTimeZone.UTC, null),
                equalTo(Relation.INTERSECTS));
        assertThat(fieldStatsProvider.isFieldWithinQuery("ip_field", "10.10.0.1", "10.10.0.60", true, false, DateTimeZone.UTC, null),
                equalTo(Relation.INTERSECTS));
        assertThat(fieldStatsProvider.isFieldWithinQuery("ip_field", "10.10.0.10", "10.20.0.1", false, true, DateTimeZone.UTC, null),
                equalTo(Relation.INTERSECTS));
        assertThat(fieldStatsProvider.isFieldWithinQuery("ip_field", "10.10.0.60", "10.20.0.1", true, true, DateTimeZone.UTC, null),
                equalTo(Relation.INTERSECTS));
        assertThat(fieldStatsProvider.isFieldWithinQuery("ip_field", "10.0.0.1", "10.10.0.10", true, true, DateTimeZone.UTC, null),
                equalTo(Relation.INTERSECTS));
        assertThat(fieldStatsProvider.isFieldWithinQuery("ip_field", "10.20.0.10", "10.20.0.1", true, true, DateTimeZone.UTC, null),
                equalTo(Relation.DISJOINT));
        assertThat(fieldStatsProvider.isFieldWithinQuery("ip_field", "10.0.0.1", "10.0.0.100", true, true, DateTimeZone.UTC, null),
                equalTo(Relation.DISJOINT));
        assertThat(fieldStatsProvider.isFieldWithinQuery("ip_field", null, "10.0.0.100", true, true, DateTimeZone.UTC, null),
                equalTo(Relation.DISJOINT));
        assertThat(fieldStatsProvider.isFieldWithinQuery("ip_field", "10.20.0.10", null, true, true, DateTimeZone.UTC, null),
                equalTo(Relation.DISJOINT));
        assertThat(fieldStatsProvider.isFieldWithinQuery("ip_field", "10.10.0.60", "10.20.0.1", false, true, DateTimeZone.UTC, null),
                equalTo(Relation.DISJOINT));
        assertThat(fieldStatsProvider.isFieldWithinQuery("ip_field", "10.0.0.1", "10.10.0.10", true, false, DateTimeZone.UTC, null),
                equalTo(Relation.DISJOINT));
    }

    private void putMapping(MapperService service) throws IOException {
        XContentBuilder mappingbuilder = JsonXContent.contentBuilder();
        mappingbuilder.startObject();
        mappingbuilder.startObject("type");
        mappingbuilder.startObject("properties");
        mappingbuilder.startObject("long_field");
        mappingbuilder.field("type", "long");
        mappingbuilder.endObject();
        mappingbuilder.startObject("float_field");
        mappingbuilder.field("type", "float");
        mappingbuilder.endObject();
        mappingbuilder.startObject("double_field");
        mappingbuilder.field("type", "double");
        mappingbuilder.endObject();
        mappingbuilder.startObject("text_field");
        mappingbuilder.field("type", "text");
        mappingbuilder.endObject();
        mappingbuilder.startObject("keyword_field");
        mappingbuilder.field("type", "keyword");
        mappingbuilder.endObject();
        mappingbuilder.startObject("date_field");
        mappingbuilder.field("type", "date");
        mappingbuilder.endObject();
        mappingbuilder.startObject("ip_field");
        mappingbuilder.field("type", "ip");
        mappingbuilder.endObject();
        mappingbuilder.endObject();
        mappingbuilder.endObject();
        mappingbuilder.endObject();
        service.merge("type", new CompressedXContent(mappingbuilder.bytes()), MergeReason.MAPPING_UPDATE, true);
    }

    private void indexDocument(MapperService service, IndexWriter writer, String id, long longValue, float floatValue, double doubleValue,
            String stringValue, DateTime dateValue, String ipValue) throws IOException {
        XContentBuilder docBuilder = JsonXContent.contentBuilder();
        docBuilder.startObject();
        docBuilder.field("long_field", longValue);
        docBuilder.field("float_field", floatValue);
        docBuilder.field("double_field", doubleValue);
        docBuilder.field("text_field", stringValue);
        docBuilder.field("keyword_field", stringValue);
        docBuilder.field("date_field", dateValue);
        docBuilder.field("ip_field", ipValue);
        docBuilder.endObject();
        DocumentMapper documentMapper = service.documentMapper("type");
        ParsedDocument doc = documentMapper.parse("index", "type", id, docBuilder.bytes());
        writer.addDocument(doc.rootDoc());
    }
}
