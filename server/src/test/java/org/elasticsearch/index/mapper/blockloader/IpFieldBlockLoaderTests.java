/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.blockloader;

import org.apache.lucene.document.InetAddressPoint;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.datageneration.FieldType;
import org.elasticsearch.index.mapper.BlockLoaderTestCase;

import java.util.List;
import java.util.Map;
import java.util.Objects;

public class IpFieldBlockLoaderTests extends BlockLoaderTestCase {
    public IpFieldBlockLoaderTests(Params params) {
        super(FieldType.IP.toString(), params);
    }

    @Override
    @SuppressWarnings("unchecked")
    protected Object expected(Map<String, Object> fieldMapping, Object value, TestContext testContext) {
        var rawNullValue = (String) fieldMapping.get("null_value");
        BytesRef nullValue = convert(rawNullValue, null);

        if (value == null) {
            return convert(null, nullValue);
        }
        if (value instanceof String s) {
            return convert(s, nullValue);
        }

        boolean hasDocValues = hasDocValues(fieldMapping, true);
        if (hasDocValues) {
            var resultList = ((List<String>) value).stream()
                .map(v -> convert(v, nullValue))
                .filter(Objects::nonNull)
                .distinct()
                .sorted()
                .toList();
            return maybeFoldList(resultList);
        }

        // field is stored or using source
        var resultList = ((List<String>) value).stream().map(v -> convert(v, nullValue)).filter(Objects::nonNull).toList();
        return maybeFoldList(resultList);
    }

    private static BytesRef convert(Object value, BytesRef nullValue) {
        if (value == null) {
            return nullValue;
        }

        if (value instanceof String s) {
            try {
                var address = InetAddresses.forString(s);
                return new BytesRef(InetAddressPoint.encode(address));
            } catch (Exception ex) {
                // malformed
                return null;
            }
        }

        return null;
    }
}
