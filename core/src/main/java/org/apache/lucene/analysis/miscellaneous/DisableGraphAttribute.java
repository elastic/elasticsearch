package org.apache.lucene.analysis.miscellaneous;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.util.Attribute;

/**
 * This attribute can be used to indicate that the {@link PositionLengthAttribute} should not be taken
 * in account in this {@link TokenStream}. Query parsers can extract this information to decide if
 * this token stream should be analyzed as a graph or not.
 */
public interface DisableGraphAttribute extends Attribute {}
