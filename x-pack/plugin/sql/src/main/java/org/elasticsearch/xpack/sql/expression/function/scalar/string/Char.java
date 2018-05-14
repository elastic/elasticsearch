/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 *  [2017] Elasticsearch Incorporated. All Rights Reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch Incorporated and its suppliers,
 * if any.  The intellectual and technical concepts contained
 * herein are proprietary to Elasticsearch Incorporated
 * and its suppliers and may be covered by U.S. and Foreign Patents,
 * patents in process, and are protected by trade secret or copyright law.
 * Dissemination of this information or reproduction of this material
 * is strictly forbidden unless prior written permission is obtained
 * from Elasticsearch Incorporated.
 */

package org.elasticsearch.xpack.sql.expression.function.scalar.string;

import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.expression.function.scalar.string.StringProcessor.StringOperation;
import org.elasticsearch.xpack.sql.tree.Location;
import org.elasticsearch.xpack.sql.tree.NodeInfo;
import org.elasticsearch.xpack.sql.type.DataType;

/**
 * Converts an int ASCII code to a character value.
 */
public class Char extends UnaryStringFunction {

    public Char(Location location, Expression field) {
        super(location, field);
    }

    @Override
    protected TypeResolution resolveType() {
        if (!childrenResolved()) {
            return new TypeResolution("Unresolved children");
        }

        return field().dataType().isInteger ? TypeResolution.TYPE_RESOLVED : new TypeResolution(
                "'%s' requires a integer type, received %s", operation(), field().dataType().esType);
    }

    @Override
    protected NodeInfo<Char> info() {
        return NodeInfo.create(this, Char::new, field());
    }

    @Override
    protected Char replaceChild(Expression newChild) {
        return new Char(location(), newChild);
    }

    @Override
    protected String formatScript(String template) {
        throw new UnsupportedOperationException("Not supported yet");
    }

    @Override
    protected StringOperation operation() {
        return StringOperation.CHAR;
    }

    @Override
    public DataType dataType() {
        return DataType.KEYWORD;
    }
}