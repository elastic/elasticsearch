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

import java.util.Locale;

import static java.lang.String.format;

/**
 * Converts an int ASCII code to a character value.
 */
public class Char extends UnaryStringFunction {

    public Char(Location location, Expression field) {
        super(location, field);
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
        return super.formatScript(
                format(Locale.ROOT, "%s instanceof Number && %s.intValue() >= 0 && %s.intValue() <= 255) ? (char) %s : null", template,
                        template, template));
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