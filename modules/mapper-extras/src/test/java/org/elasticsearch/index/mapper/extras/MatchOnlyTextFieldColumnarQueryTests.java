/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.extras;

import org.apache.lucene.search.Query;
import org.apache.lucene.util.automaton.Operations;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.codec.columnar.ColumnarDocValuesFormatSelector;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MapperServiceTestCase;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.plugins.Plugin;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

import static org.hamcrest.Matchers.startsWith;

/**
 * A field whose doc values are a ColumNAR column has to be queried through the columnar queries, and a scanning
 * query built for one refuses to be constructed. These are the query methods on a match_only_text field with no inverted index
 * to answer them from, so every one of them has to route by format rather than reaching for a scanning query.
 */
public class MatchOnlyTextFieldColumnarQueryTests extends MapperServiceTestCase {

    private static final String FIELD = "field";

    @Override
    protected Collection<Plugin> getPlugins() {
        return List.of(new MapperExtrasPlugin());
    }

    public void testEveryQueryIsAnsweredByTheColumn() throws IOException {
        assumeTrue("columnar_codec feature flag must be enabled", ColumnarDocValuesFormatSelector.COLUMNAR_CODEC_FEATURE_FLAG.isEnabled());
        final MapperService mapperService = columnarMapperService();
        final SearchExecutionContext context = createSearchExecutionContext(mapperService);
        final MappedFieldType field = mapperService.fieldType(FIELD);

        assertColumnar(field.termQuery("a", context));
        assertColumnar(field.termsQuery(List.of("a", "b"), context));
        assertColumnar(field.prefixQuery("a", null, false, context));
        assertColumnar(field.wildcardQuery("a*b", null, false, context));
        assertColumnar(field.regexpQuery("a.*", 0, 0, Operations.DEFAULT_DETERMINIZE_WORK_LIMIT, null, context));
    }

    /**
     * Any of the columnar queries will do. What matters is that a scanning one was not built, which
     * {@link org.elasticsearch.lucene.queries.AbstractBinaryDocValuesQuery} refuses for a column anyway, so this
     * fails as a thrown exception rather than a wrong class when the routing is missed.
     */
    private static void assertColumnar(Query query) {
        assertThat(query.getClass().getName(), startsWith("org.elasticsearch.columnar."));
    }

    private MapperService columnarMapperService() throws IOException {
        final Settings settings = Settings.builder()
            .put(IndexSettings.MODE.getKey(), IndexMode.COLUMNAR.getName())
            .put(IndexSettings.COLUMNAR_CODEC_ENABLED_SETTING.getKey(), true)
            .build();
        // No inverted index, so these queries are answered from the doc values rather than from terms.
        return createMapperService(
            settings,
            mapping(b -> b.startObject(FIELD).field("type", "match_only_text").field("index", false).field("doc_values", true).endObject())
        );
    }
}
