/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.ml.inference;

import org.elasticsearch.common.xcontent.ToXContentObject;

/**
 * Simple interface for XContent Objects that are named.
 *
 * This affords more general handling when serializing and de-serializing this type of XContent when it is used in a NamedObjects
 * parser.
 */
public interface NamedXContentObject extends ToXContentObject {
    /**
     * @return The name of the XContentObject that is to be serialized
     */
    String getName();
}
