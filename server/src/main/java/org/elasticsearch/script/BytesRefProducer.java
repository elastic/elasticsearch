/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script;

import org.apache.lucene.util.BytesRef;

// decouples this module from org.elasticsearch.xpack.versionfield.Version field type
// that is defined in x-pack
public interface BytesRefProducer {

    BytesRef toBytesRef();
}
