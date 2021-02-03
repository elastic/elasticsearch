/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.index.mapper;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.test.ESTestCase;

import static java.util.Collections.emptyMap;

public class FieldNamesFieldTypeTests extends ESTestCase {

    public void testTermQuery() {

        FieldNamesFieldMapper.FieldNamesFieldType fieldNamesFieldType = new FieldNamesFieldMapper.FieldNamesFieldType(true);
        KeywordFieldMapper.KeywordFieldType fieldType = new KeywordFieldMapper.KeywordFieldType("field_name");

        Settings settings = settings(Version.CURRENT).build();
        IndexSettings indexSettings = new IndexSettings(
                new IndexMetadata.Builder("foo").settings(settings).numberOfShards(1).numberOfReplicas(0).build(), settings);
        MappingLookup mappingLookup = MappingLookupUtils.fromTypes(fieldNamesFieldType, fieldType);

        QueryShardContext queryShardContext = new QueryShardContext(0,
                indexSettings, BigArrays.NON_RECYCLING_INSTANCE, null, null, null, mappingLookup,
                null, null, null, null, null, null, () -> 0L, null, null, () -> true, null, emptyMap());
                Query termQuery = fieldNamesFieldType.termQuery("field_name", queryShardContext);
        assertEquals(new TermQuery(new Term(FieldNamesFieldMapper.CONTENT_TYPE, "field_name")), termQuery);
        assertWarnings("terms query on the _field_names field is deprecated and will be removed, use exists query instead");

        FieldNamesFieldMapper.FieldNamesFieldType unsearchable = new FieldNamesFieldMapper.FieldNamesFieldType(false);
        IllegalStateException e = expectThrows(IllegalStateException.class, () -> unsearchable.termQuery("field_name", null));
        assertEquals("Cannot run [exists] queries if the [_field_names] field is disabled", e.getMessage());
    }
}
