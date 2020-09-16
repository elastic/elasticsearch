/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.runtimefields.query;

import org.apache.lucene.document.InetAddressPoint;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.script.Script;
import org.elasticsearch.xpack.runtimefields.IpScript;
import org.elasticsearch.xpack.runtimefields.StringScript;

import java.net.InetAddress;

/**
 * Abstract base class for building queries based on {@link StringScript}.
 */
abstract class AbstractIpScriptFieldQuery extends AbstractScriptFieldQuery<IpScript> {

    AbstractIpScriptFieldQuery(Script script, IpScript.LeafFactory leafFactory, String fieldName) {
        super(script, fieldName, leafFactory::newInstance);
    }

    @Override
    protected boolean matches(IpScript scriptContext, int docId) {
        scriptContext.runForDoc(docId);
        return matches(scriptContext.values(), scriptContext.count());
    }

    /**
     * Does the value match this query?
     */
    protected abstract boolean matches(BytesRef[] values, int conut);

    protected static InetAddress decode(BytesRef ref) {
        return InetAddressPoint.decode(BytesReference.toBytes(new BytesArray(ref)));
    }
}
