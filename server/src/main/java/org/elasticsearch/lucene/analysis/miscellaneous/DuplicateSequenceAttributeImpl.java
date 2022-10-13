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

public class DuplicateSequenceAttributeImpl extends AttributeImpl implements DuplicateSequenceAttribute {
    protected short numPriorUsesInASequence = 0;

    @Override
    public void clear() {
        numPriorUsesInASequence = 0;
    }

    @Override
    public void copyTo(AttributeImpl target) {
        DuplicateSequenceAttributeImpl t = (DuplicateSequenceAttributeImpl) target;
        t.numPriorUsesInASequence = numPriorUsesInASequence;
    }

    @Override
    public short getNumPriorUsesInASequence() {
        return numPriorUsesInASequence;
    }

    @Override
    public void setNumPriorUsesInASequence(short len) {
        numPriorUsesInASequence = len;
    }

    @Override
    public void reflectWith(AttributeReflector reflector) {
        reflector.reflect(DuplicateSequenceAttribute.class, "sequenceLength", numPriorUsesInASequence);
    }
}
