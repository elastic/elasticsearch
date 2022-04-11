/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ql.expression.predicate.regex;

import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.Nullability;
import org.elasticsearch.xpack.ql.expression.function.scalar.UnaryScalarFunction;
import org.elasticsearch.xpack.ql.expression.gen.processor.Processor;
import org.elasticsearch.xpack.ql.expression.gen.script.ScriptTemplate;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;

import java.util.Objects;

import static org.elasticsearch.common.logging.LoggerMessageFormat.format;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.ParamOrdinal.DEFAULT;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.isStringAndExact;
import static org.elasticsearch.xpack.ql.expression.gen.script.ParamsBuilder.paramsBuilder;

public abstract class RegexMatch<T extends StringPattern> extends UnaryScalarFunction {

    private final T pattern;
    private final boolean caseInsensitive;

    protected RegexMatch(Source source, Expression value, T pattern, boolean caseInsensitive) {
        super(source, value);
        this.pattern = pattern;
        this.caseInsensitive = caseInsensitive;
    }

    public T pattern() {
        return pattern;
    }

    public boolean caseInsensitive() {
        return caseInsensitive;
    }

    @Override
    public DataType dataType() {
        return DataTypes.BOOLEAN;
    }

    @Override
    public Nullability nullable() {
        if (pattern() == null) {
            return Nullability.TRUE;
        }
        return field().nullable();
    }

    @Override
    protected TypeResolution resolveType() {
        return isStringAndExact(field(), sourceText(), DEFAULT);
    }

    @Override
    public boolean foldable() {
        // right() is not directly foldable in any context but Like can fold it.
        return field().foldable();
    }

    @Override
    public Boolean fold() {
        Object val = field().fold();
        return RegexProcessor.RegexOperation.match(val, pattern().asJavaRegex());
    }

    @Override
    protected Processor makeProcessor() {
        return new RegexProcessor(pattern().asJavaRegex());
    }

    @Override
    public ScriptTemplate asScript() {
        ScriptTemplate fieldAsScript = asScript(field());
        // keep backwards compatibility with previous 7.x versions
        if (caseInsensitive == false) {
            return new ScriptTemplate(
                formatTemplate(format("{ql}.", "regex({},{})", fieldAsScript.template())),
                paramsBuilder().script(fieldAsScript.params()).variable(pattern.asJavaRegex()).build(),
                dataType()
            );
        }
        return new ScriptTemplate(
            formatTemplate(format("{ql}.", "regex({},{},{})", fieldAsScript.template())),
            paramsBuilder().script(fieldAsScript.params()).variable(pattern.asJavaRegex()).variable(caseInsensitive).build(),
            dataType()
        );
    }

    @Override
    public boolean equals(Object obj) {
        if (super.equals(obj)) {
            RegexMatch<?> other = (RegexMatch<?>) obj;
            return caseInsensitive == other.caseInsensitive && Objects.equals(pattern, other.pattern);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), pattern(), caseInsensitive);
    }
}
