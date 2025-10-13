/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import com.carrotsearch.randomizedtesting.generators.RandomStrings;

import org.elasticsearch.common.network.NetworkAddress;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;

public class IPSyntheticSourceNativeArrayIntegrationTests extends NativeArrayIntegrationTestCase {

    @Override
    protected String getFieldTypeName() {
        return "ip";
    }

    @Override
    protected String getRandomValue() {
        return NetworkAddress.format(randomIp(true));
    }

    @Override
    protected String getMalformedValue() {
        return RandomStrings.randomAsciiOfLength(random(), 8);
    }

    public void testSynthesizeArray() throws Exception {
        var arrayValues = new Object[][] {
            new Object[] { "192.168.1.4", "192.168.1.3", null, "192.168.1.2", null, "192.168.1.1" },
            new Object[] { null, "192.168.1.2", null, "192.168.1.1" },
            new Object[] { null },
            new Object[] { null, null, null },
            new Object[] { "192.168.1.3", "192.168.1.2", "192.168.1.1" } };
        verifySyntheticArray(arrayValues);
    }

    public void testSynthesizeArrayIgnoreMalformed() throws Exception {
        var mapping = jsonBuilder().startObject()
            .startObject("properties")
            .startObject("field")
            .field("type", "ip")
            .field("ignore_malformed", true)
            .endObject()
            .endObject()
            .endObject();
        // Note values that would be ignored are added at the end of arrays,
        // this makes testing easier as ignored values are always synthesized after regular values:
        var arrayValues = new Object[][] {
            new Object[] { null, "192.168.1.1", "192.168.1.2", "192.168.1.3", "192.168.1.4", null, "malformed" },
            new Object[] { "192.168.1.1", "192.168.1.2", "malformed" },
            new Object[] { "192.168.1.1", "192.168.1.1", "malformed" },
            new Object[] { null, null, null, "malformed" },
            new Object[] { "192.168.1.3", "192.168.1.3", "192.168.1.1", "malformed" } };
        verifySyntheticArray(arrayValues, mapping, "_id", "field._ignore_malformed");
    }

    public void testSynthesizeObjectArray() throws Exception {
        List<List<Object[]>> documents = new ArrayList<>();
        {
            List<Object[]> document = new ArrayList<>();
            document.add(new Object[] { "192.168.1.3", "192.168.1.2", "192.168.1.1" });
            document.add(new Object[] { "192.168.1.110", "192.168.1.109", "192.168.1.111" });
            document.add(new Object[] { "192.168.1.2", "192.168.1.2", "192.168.1.1" });
            documents.add(document);
        }
        {
            List<Object[]> document = new ArrayList<>();
            document.add(new Object[] { "192.168.1.9", "192.168.1.7", "192.168.1.5" });
            document.add(new Object[] { "192.168.1.2", "192.168.1.4", "192.168.1.6" });
            document.add(new Object[] { "192.168.1.7", "192.168.1.6", "192.168.1.5" });
            documents.add(document);
        }
        verifySyntheticObjectArray(documents);
    }

    public void testSynthesizeArrayInObjectField() throws Exception {
        List<Object[]> documents = new ArrayList<>();
        documents.add(new Object[] { "192.168.1.254", "192.168.1.253", "192.168.1.252" });
        documents.add(new Object[] { "192.168.1.112", "192.168.1.113", "192.168.1.114" });
        documents.add(new Object[] { "192.168.1.3", "192.168.1.2", "192.168.1.1" });
        documents.add(new Object[] { "192.168.1.9", "192.168.1.7", "192.168.1.5" });
        documents.add(new Object[] { "192.168.1.2", "192.168.1.4", "192.168.1.6" });
        documents.add(new Object[] { "192.168.1.7", "192.168.1.6", "192.168.1.5" });
        verifySyntheticArrayInObject(documents);
    }

}
