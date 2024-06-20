/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.bulk;

import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.test.ESTestCase;

import java.util.Collections;

public class TransportAbstractBulkActionTests extends ESTestCase {

    public void testGetIndexWriteRequest() {
        IndexRequest indexRequest = new IndexRequest("index").id("id1").source(Collections.emptyMap());
        UpdateRequest upsertRequest = new UpdateRequest("index", "id1").upsert(indexRequest).script(mockScript("1"));
        UpdateRequest docAsUpsertRequest = new UpdateRequest("index", "id2").doc(indexRequest).docAsUpsert(true);
        UpdateRequest scriptedUpsert = new UpdateRequest("index", "id2").upsert(indexRequest).script(mockScript("1")).scriptedUpsert(true);

        assertEquals(TransportAbstractBulkAction.getIndexWriteRequest(indexRequest), indexRequest);
        assertEquals(TransportAbstractBulkAction.getIndexWriteRequest(upsertRequest), indexRequest);
        assertEquals(TransportAbstractBulkAction.getIndexWriteRequest(docAsUpsertRequest), indexRequest);
        assertEquals(TransportAbstractBulkAction.getIndexWriteRequest(scriptedUpsert), indexRequest);

        DeleteRequest deleteRequest = new DeleteRequest("index", "id");
        assertNull(TransportAbstractBulkAction.getIndexWriteRequest(deleteRequest));

        UpdateRequest badUpsertRequest = new UpdateRequest("index", "id1");
        assertNull(TransportAbstractBulkAction.getIndexWriteRequest(badUpsertRequest));
    }
}
