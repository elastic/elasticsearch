package org.apache.lucene.analysis.miscellaneous;

import org.apache.lucene.util.AttributeImpl;
import org.apache.lucene.util.AttributeReflector;

/** Default implementation of {@link DisableGraphAttribute}. */
public class DisableGraphAttributeImpl extends AttributeImpl implements DisableGraphAttribute {
    public DisableGraphAttributeImpl() {}

    @Override
    public void clear() {}

    @Override
    public void reflectWith(AttributeReflector reflector) {
    }

    @Override
    public void copyTo(AttributeImpl target) {}
}
