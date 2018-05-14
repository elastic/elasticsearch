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
import org.elasticsearch.xpack.sql.expression.function.scalar.UnaryScalarFunction;
import org.elasticsearch.xpack.sql.expression.function.scalar.processor.definition.ProcessorDefinition;
import org.elasticsearch.xpack.sql.expression.function.scalar.processor.definition.ProcessorDefinitions;
import org.elasticsearch.xpack.sql.expression.function.scalar.processor.definition.UnaryProcessorDefinition;
import org.elasticsearch.xpack.sql.expression.function.scalar.string.StringProcessor.StringOperation;
import org.elasticsearch.xpack.sql.tree.Location;

import java.util.Objects;

public abstract class UnaryStringFunction extends UnaryScalarFunction {

    protected UnaryStringFunction(Location location, Expression field) {
        super(location, field);
    }

    @Override
    public boolean foldable() {
        return field().foldable();
    }

    @Override
    public Object fold() {
        return operation().apply(field().fold());
    }

    @Override
    protected TypeResolution resolveType() {
        if (!childrenResolved()) {
            return new TypeResolution("Unresolved children");
        }

        return field().dataType().isString() ? TypeResolution.TYPE_RESOLVED : new TypeResolution(
                "'%s' requires a string type, received %s", operation(), field().dataType().esType);
    }

    @Override
    protected final ProcessorDefinition makeProcessorDefinition() {
        return new UnaryProcessorDefinition(location(), this, ProcessorDefinitions.toProcessorDefinition(field()),
                new StringProcessor(operation()));
    }

    protected abstract StringOperation operation();

    @Override
    public boolean equals(Object obj) {
        if (obj == null || obj.getClass() != getClass()) {
            return false;
        }
        UnaryStringFunction other = (UnaryStringFunction) obj;
        return Objects.equals(other.field(), field());
    }

    @Override
    public int hashCode() {
        return Objects.hash(field());
    }
}