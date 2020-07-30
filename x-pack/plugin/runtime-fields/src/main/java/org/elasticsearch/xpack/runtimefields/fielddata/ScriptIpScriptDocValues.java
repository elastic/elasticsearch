/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.runtimefields.fielddata;

import org.apache.lucene.document.InetAddressPoint;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.xpack.runtimefields.IpScriptFieldScript;

import java.io.IOException;
import java.net.InetAddress;

public class ScriptIpScriptDocValues extends ScriptDocValues<String> {
    private final IpScriptFieldScript script;

    public ScriptIpScriptDocValues(IpScriptFieldScript script) {
        this.script = script;
    }

    @Override
    public void setNextDocId(int docId) throws IOException {
        script.runForDoc(docId);
    }

    public String getValue() {
        if (size() == 0) {
            return null;
        }
        return get(0);
    }

    @Override
    public String get(int index) {
        if (index >= size()) {
            if (size() == 0) {
                throw new IllegalStateException(
                    "A document doesn't have a value for a field! "
                        + "Use doc[<field>].size()==0 to check if a document is missing a field!"
                );
            }
            throw new ArrayIndexOutOfBoundsException("There are only [" + size() + "] values.");
        }
        InetAddress addr = InetAddressPoint.decode(BytesReference.toBytes(new BytesArray(script.values()[index])));
        return InetAddresses.toAddrString(addr);
    }

    @Override
    public int size() {
        return script.count();
    }

}
