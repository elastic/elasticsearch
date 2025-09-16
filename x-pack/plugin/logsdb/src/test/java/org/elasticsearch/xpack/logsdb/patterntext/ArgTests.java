/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb.patterntext;

import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.ByteArrayDataOutput;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ArgTests extends ESTestCase {

    public void testInfoRoundTrip() throws IOException {
        byte[] buf = new byte[15];
        var output = new ByteArrayDataOutput(buf);
        int previousOffset = randomIntBetween(0, 100);
        Arg.Info info = randomInfo();
        info.writeTo(output, previousOffset);

        var input = new ByteArrayDataInput(buf);
        Arg.Info actual = Arg.Info.readFrom(input, previousOffset);
        assertEquals(info, actual);
    }

    public void testInfoListRoundTrip() throws IOException {
        int numArgs = randomIntBetween(0, 100);
        var infoList = new ArrayList<Arg.Info>();
        for (int i = 0; i < numArgs; i++) {
            infoList.add(randomInfo());
        }

        String encoded = Arg.encodeInfo(infoList);
        List<Arg.Info> actual = Arg.decodeInfo(encoded);

        assertEquals(infoList.size(), actual.size());
        assertArrayEquals(infoList.toArray(), actual.toArray());
    }

    Arg.Info randomInfo() {
        return new Arg.Info(randomFrom(Arg.Type.values()), randomIntBetween(0, 10_000));
    }
}
