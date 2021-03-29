/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.runtime;

import org.apache.lucene.document.InetAddressPoint;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.script.IpFieldScript;
import org.elasticsearch.script.StringFieldScript;
import org.elasticsearch.script.Script;

import java.net.InetAddress;

/**
 * Abstract base class for building queries based on {@link StringFieldScript}.
 */
abstract class AbstractIpScriptFieldQuery extends AbstractScriptFieldQuery<IpFieldScript> {

    AbstractIpScriptFieldQuery(Script script, IpFieldScript.LeafFactory leafFactory, String fieldName) {
        super(script, fieldName, leafFactory::newInstance);
    }

    @Override
    protected boolean matches(IpFieldScript scriptContext, int docId) {
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
