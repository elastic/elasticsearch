/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.plan.physical.DocRefEncodeExec;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeSinkExec;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeSourceExec;

import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public class DataNodeComputeHandlerTests extends ESTestCase {

    /**
     * Rows that leave a data node as document references name a context the fetch phase finds again. A request without
     * fetch contexts would name contexts that close with the request.
     */
    public void testDocumentReferencesNeedFetchContexts() {
        Attribute doc = new MetadataAttribute(Source.EMPTY, MetadataAttribute.DOC, DataType.DOC_DATA_TYPE, false);
        ReferenceAttribute docRef = new ReferenceAttribute(
            Source.EMPTY,
            null,
            "$$doc_ref",
            DataType.DOC_REF,
            Nullability.FALSE,
            null,
            true
        );
        DocRefEncodeExec encode = new DocRefEncodeExec(
            Source.EMPTY,
            new ExchangeSourceExec(Source.EMPTY, List.of(doc), false),
            doc,
            docRef
        );
        ExchangeSinkExec plan = new ExchangeSinkExec(Source.EMPTY, encode.output(), false, encode);

        IllegalStateException e = expectThrows(
            IllegalStateException.class,
            () -> DataNodeComputeHandler.validateFetchContexts(plan, request(plan, false))
        );
        assertThat(e.getMessage(), equalTo("document references need fetch contexts, but the request asked for none"));
        DataNodeComputeHandler.validateFetchContexts(plan, request(plan, true));
    }

    private static DataNodeRequest request(ExchangeSinkExec plan, boolean keepFetchContexts) {
        return new DataNodeRequest(
            "session",
            EsqlTestUtils.TEST_CFG,
            "",
            List.of(),
            Map.of(),
            plan,
            new String[0],
            IndicesOptions.STRICT_EXPAND_OPEN,
            true,
            true,
            false,
            List.of(),
            keepFetchContexts
        );
    }
}
