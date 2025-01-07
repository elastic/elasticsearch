/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.fetch.subphase;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.memory.MemoryIndex;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.fetch.FetchContext;
import org.elasticsearch.search.fetch.FetchSubPhase.HitContext;
import org.elasticsearch.search.fetch.FetchSubPhaseProcessor;
import org.elasticsearch.search.lookup.Source;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class FetchSourcePhaseTests extends ESTestCase {

    public void testFetchSource() throws IOException {
        XContentBuilder source = XContentFactory.jsonBuilder().startObject().field("field", "value").endObject();
        HitContext hitContext = hitExecute(source, true, null, null);
        assertEquals(Collections.singletonMap("field", "value"), hitContext.hit().getSourceAsMap());
    }

    public void testBasicFiltering() throws IOException {
        XContentBuilder source = XContentFactory.jsonBuilder().startObject().field("field1", "value").field("field2", "value2").endObject();
        HitContext hitContext = hitExecute(source, false, null, null);
        assertNull(hitContext.hit().getSourceAsMap());

        hitContext = hitExecute(source, true, "field1", null);
        assertEquals(Collections.singletonMap("field1", "value"), hitContext.hit().getSourceAsMap());

        hitContext = hitExecute(source, true, "hello", null);
        assertEquals(Collections.emptyMap(), hitContext.hit().getSourceAsMap());

        hitContext = hitExecute(source, true, "*", "field2");
        assertEquals(Collections.singletonMap("field1", "value"), hitContext.hit().getSourceAsMap());
    }

    public void testExcludesAll() throws IOException {
        XContentBuilder source = XContentFactory.jsonBuilder().startObject().field("field1", "value").field("field2", "value2").endObject();
        HitContext hitContext = hitExecute(source, false, null, null);
        assertNull(hitContext.hit().getSourceAsMap());

        hitContext = hitExecute(source, true, "field1", "*");
        assertEquals(Collections.emptyMap(), hitContext.hit().getSourceAsMap());

        hitContext = hitExecute(source, true, null, "*");
        assertEquals(Collections.emptyMap(), hitContext.hit().getSourceAsMap());

        hitContext = hitExecute(source, true, "*", "*");
        assertEquals(Collections.emptyMap(), hitContext.hit().getSourceAsMap());

        hitContext = hitExecuteMultiple(source, true, new String[] { "field1", "field2" }, new String[] { "*", "field1" });
        assertEquals(Collections.emptyMap(), hitContext.hit().getSourceAsMap());

        hitContext = hitExecuteMultiple(source, true, null, new String[] { "field2", "*", "field1" });
        assertEquals(Collections.emptyMap(), hitContext.hit().getSourceAsMap());
    }

    public void testMultipleFiltering() throws IOException {
        XContentBuilder source = XContentFactory.jsonBuilder().startObject().field("field", "value").field("field2", "value2").endObject();
        HitContext hitContext = hitExecuteMultiple(source, true, new String[] { "*.notexisting", "field" }, null);
        assertEquals(Collections.singletonMap("field", "value"), hitContext.hit().getSourceAsMap());

        hitContext = hitExecuteMultiple(source, true, new String[] { "field.notexisting.*", "field" }, null);
        assertEquals(Collections.singletonMap("field", "value"), hitContext.hit().getSourceAsMap());
    }

    public void testNestedSource() throws IOException {
        Map<String, Object> expectedNested = Collections.singletonMap("nested2", Collections.singletonMap("field", "value0"));
        XContentBuilder source = XContentFactory.jsonBuilder()
            .startObject()
            .field("field", "value")
            .field("field2", "value2")
            .field("nested1", expectedNested)
            .endObject();
        HitContext hitContext = hitExecuteMultiple(source, true, null, null, new SearchHit.NestedIdentity("nested1", 0, null));
        assertEquals(expectedNested, hitContext.hit().getSourceAsMap());
        hitContext = hitExecuteMultiple(source, true, new String[] { "invalid" }, null, new SearchHit.NestedIdentity("nested1", 0, null));
        assertEquals(Collections.emptyMap(), hitContext.hit().getSourceAsMap());

        hitContext = hitExecuteMultiple(
            source,
            true,
            null,
            null,
            new SearchHit.NestedIdentity("nested1", 0, new SearchHit.NestedIdentity("nested2", 0, null))
        );
        assertEquals(Collections.singletonMap("field", "value0"), hitContext.hit().getSourceAsMap());

        hitContext = hitExecuteMultiple(
            source,
            true,
            new String[] { "invalid" },
            null,
            new SearchHit.NestedIdentity("nested1", 0, new SearchHit.NestedIdentity("nested2", 0, null))
        );
        assertEquals(Collections.emptyMap(), hitContext.hit().getSourceAsMap());
    }

    public void testSourceDisabled() throws IOException {
        HitContext hitContext = hitExecute(null, true, null, null);
        assertNull(hitContext.hit().getSourceAsMap());

        hitContext = hitExecute(null, false, null, null);
        assertNull(hitContext.hit().getSourceAsMap());

        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> hitExecute(null, true, "field1", null));
        assertEquals(
            "unable to fetch fields from _source field: _source is disabled in the mappings " + "for index [index]",
            exception.getMessage()
        );

        exception = expectThrows(
            IllegalArgumentException.class,
            () -> hitExecuteMultiple(null, true, new String[] { "*" }, new String[] { "field2" })
        );
        assertEquals(
            "unable to fetch fields from _source field: _source is disabled in the mappings " + "for index [index]",
            exception.getMessage()
        );
    }

    public void testNestedSourceWithSourceDisabled() throws IOException {
        HitContext hitContext = hitExecute(null, true, null, null, new SearchHit.NestedIdentity("nested1", 0, null));
        assertNull(hitContext.hit().getSourceAsMap());

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> hitExecute(null, true, "field1", null, new SearchHit.NestedIdentity("nested1", 0, null))
        );
        assertEquals(
            "unable to fetch fields from _source field: _source is disabled in the mappings " + "for index [index]",
            e.getMessage()
        );
    }

    private HitContext hitExecute(XContentBuilder source, boolean fetchSource, String include, String exclude) throws IOException {
        return hitExecute(source, fetchSource, include, exclude, null);
    }

    private HitContext hitExecute(
        XContentBuilder source,
        boolean fetchSource,
        String include,
        String exclude,
        SearchHit.NestedIdentity nestedIdentity
    ) throws IOException {
        return hitExecuteMultiple(
            source,
            fetchSource,
            include == null ? Strings.EMPTY_ARRAY : new String[] { include },
            exclude == null ? Strings.EMPTY_ARRAY : new String[] { exclude },
            nestedIdentity
        );
    }

    private HitContext hitExecuteMultiple(XContentBuilder source, boolean fetchSource, String[] includes, String[] excludes)
        throws IOException {
        return hitExecuteMultiple(source, fetchSource, includes, excludes, null);
    }

    private HitContext hitExecuteMultiple(
        XContentBuilder sourceBuilder,
        boolean fetchSource,
        String[] includes,
        String[] excludes,
        SearchHit.NestedIdentity nestedIdentity
    ) throws IOException {
        FetchSourceContext fetchSourceContext = FetchSourceContext.of(fetchSource, includes, excludes);
        FetchContext fetchContext = mock(FetchContext.class);
        when(fetchContext.fetchSourceContext()).thenReturn(fetchSourceContext);
        when(fetchContext.getIndexName()).thenReturn("index");
        SearchExecutionContext sec = mock(SearchExecutionContext.class);
        when(sec.isSourceEnabled()).thenReturn(sourceBuilder != null);
        IndexSettings indexSettings = new IndexSettings(
            IndexMetadata.builder("index").settings(indexSettings(IndexVersion.current(), 1, 0)).build(),
            Settings.EMPTY
        );
        when(sec.indexVersionCreated()).thenReturn(indexSettings.getIndexVersionCreated());
        when(sec.getIndexSettings()).thenReturn(indexSettings);
        when(fetchContext.getSearchExecutionContext()).thenReturn(sec);

        final SearchHit searchHit = SearchHit.unpooled(1, null, nestedIdentity);

        // We don't need a real index, just a LeafReaderContext which cannot be mocked.
        MemoryIndex index = new MemoryIndex();
        LeafReaderContext leafReaderContext = index.createSearcher().getIndexReader().leaves().get(0);
        Source source = sourceBuilder == null ? Source.empty(null) : Source.fromBytes(BytesReference.bytes(sourceBuilder));
        HitContext hitContext = new HitContext(searchHit, leafReaderContext, 1, Map.of(), source, null);

        FetchSourcePhase phase = new FetchSourcePhase();
        FetchSubPhaseProcessor processor = phase.getProcessor(fetchContext);
        if (fetchSource == false) {
            assertNull(processor);
        } else {
            assertNotNull(processor);
            processor.process(hitContext);
        }
        return hitContext;
    }

}
