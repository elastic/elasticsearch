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
import org.elasticsearch.xpack.sql.expression.Expression.TypeResolution;
import org.elasticsearch.xpack.sql.expression.function.scalar.UnaryScalarFunction;
import org.elasticsearch.xpack.sql.expression.function.scalar.string.StringProcessor.StringOperation;
import org.elasticsearch.xpack.sql.tree.Location;
import org.elasticsearch.xpack.sql.tree.NodeInfo;
import org.elasticsearch.xpack.sql.type.DataType;

/**
 * Trims the leading whitespaces.
 */
public class LTrim extends UnaryStringFunction {

    public LTrim(Location location, Expression field) {
        super(location, field);
    }

    @Override
    protected NodeInfo<LTrim> info() {
        return NodeInfo.create(this, LTrim::new, field());
    }

    @Override
    protected LTrim replaceChild(Expression newChild) {
        return new LTrim(location(), newChild);
    }

    @Override
    protected String formatScript(String template) {
        throw new UnsupportedOperationException("Not supported yet");
    }

    @Override
    protected StringOperation operation() {
        return StringOperation.LTRIM;
    }

    @Override
    public DataType dataType() {
        return DataType.KEYWORD;
    }

}
