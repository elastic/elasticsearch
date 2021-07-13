/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.fielddata;

import org.apache.lucene.document.InetAddressPoint;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.index.mapper.IpFieldMapper;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.script.IpFieldScript;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;

import java.net.InetAddress;

public class IpScriptFieldData extends BinaryScriptFieldData {
    public static class Builder implements IndexFieldData.Builder {
        private final String name;
        private final IpFieldScript.LeafFactory leafFactory;

        public Builder(String name, IpFieldScript.LeafFactory leafFactory) {
            this.name = name;
            this.leafFactory = leafFactory;
        }

        @Override
        public IpScriptFieldData build(IndexFieldDataCache cache, CircuitBreakerService breakerService) {
            return new IpScriptFieldData(name, leafFactory);
        }
    }

    private final IpFieldScript.LeafFactory leafFactory;

    private IpScriptFieldData(String fieldName, IpFieldScript.LeafFactory leafFactory) {
        super(fieldName);
        this.leafFactory = leafFactory;
    }

    @Override
    public BinaryScriptLeafFieldData loadDirect(LeafReaderContext context) throws Exception {
        IpFieldScript script = leafFactory.newInstance(context);
        return new BinaryScriptLeafFieldData() {
            @Override
            public ScriptDocValues<String> getScriptValues() {
                return new IpScriptDocValues(getBytesValues());
            }

            @Override
            public SortedBinaryDocValues getBytesValues() {
                return new org.elasticsearch.index.fielddata.IpScriptDocValues(script);
            }
        };
    }

    @Override
    public ValuesSourceType getValuesSourceType() {
        return CoreValuesSourceType.IP;
    }

    /**
     * Doc values implementation for ips. We can't share
     * {@link IpFieldMapper.IpFieldType.IpScriptDocValues} because it is based
     * on global ordinals and we don't have those.
     */
    public static class IpScriptDocValues extends ScriptDocValues.Strings {
        public IpScriptDocValues(SortedBinaryDocValues in) {
            super(in);
        }

        @Override
        protected String bytesToString(BytesRef bytes) {
            InetAddress addr = InetAddressPoint.decode(BytesReference.toBytes(new BytesArray(bytes)));
            return InetAddresses.toAddrString(addr);
        }
    }
}
