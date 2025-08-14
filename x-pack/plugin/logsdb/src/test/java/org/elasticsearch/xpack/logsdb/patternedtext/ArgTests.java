/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb.patternedtext;

import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.ByteArrayDataOutput;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ArgTests extends ESTestCase {

    public void testSchemaRoundTrip() throws IOException {
        byte[] buf = new byte[15];
        var output = new ByteArrayDataOutput(buf);
        Arg.Schema schema = randomSchema();
        schema.writeTo(output);

        var input = new ByteArrayDataInput(buf);
        Arg.Schema actual = Arg.Schema.readFrom(input);
        assertEquals(schema, actual);
    }

    public void testSchemaListRoundTrip() throws IOException {
        int numArgs = randomIntBetween(0, 100);
        var schemaList = new ArrayList<Arg.Schema>();
        for (int i = 0; i < numArgs; i++) {
            schemaList.add(randomSchema());
        }

        String encoded = Arg.encodeSchema(schemaList);
        List<Arg.Schema> actual = Arg.decodeSchema(encoded);

        assertEquals(schemaList.size(), actual.size());
        assertArrayEquals(schemaList.toArray(), actual.toArray());
    }

    Arg.Schema randomSchema() {
        return new Arg.Schema(randomFrom(Arg.Type.values()), randomIntBetween(0, 10_000));
    }
}
