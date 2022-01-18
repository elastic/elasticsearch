/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.lucene.analysis.miscellaneous;

import org.apache.lucene.util.AttributeImpl;
import org.apache.lucene.util.AttributeReflector;

/** Default implementation of {@link DisableGraphAttribute}. */
public class DisableGraphAttributeImpl extends AttributeImpl implements DisableGraphAttribute {
    public DisableGraphAttributeImpl() {}

    @Override
    public void clear() {}

    @Override
    public void reflectWith(AttributeReflector reflector) {}

    @Override
    public void copyTo(AttributeImpl target) {}
}
