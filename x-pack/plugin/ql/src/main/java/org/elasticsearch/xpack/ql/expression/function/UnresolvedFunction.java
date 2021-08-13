/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ql.expression.function;

import org.elasticsearch.xpack.ql.capabilities.Unresolvable;
import org.elasticsearch.xpack.ql.capabilities.UnresolvedException;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.Nullability;
import org.elasticsearch.xpack.ql.expression.gen.script.ScriptTemplate;
import org.elasticsearch.xpack.ql.session.Configuration;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.util.StringUtils;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

public class UnresolvedFunction extends Function implements Unresolvable {

    private final String name;
    private final String unresolvedMsg;
    private final FunctionResolutionStrategy resolution;

    /**
     * Flag to indicate analysis has been applied and there's no point in
     * doing it again this is an optimization to prevent searching for a
     * better unresolved message over and over again.
     */
    private final boolean analyzed;

    public UnresolvedFunction(Source source, String name, FunctionResolutionStrategy resolutionStrategy, List<Expression> children) {
        this(source, name, resolutionStrategy, children, false, null);
    }

    /**
     * Constructor used for specifying a more descriptive message (typically
     * 'did you mean') instead of the default one.
     *
     * @see #withMessage(String)
     */
    UnresolvedFunction(Source source, String name, FunctionResolutionStrategy resolutionStrategy, List<Expression> children,
                       boolean analyzed, String unresolvedMessage) {
        super(source, children);
        this.name = name;
        this.resolution = resolutionStrategy;
        this.analyzed = analyzed;
        this.unresolvedMsg = unresolvedMessage == null ? "Unknown " + resolutionStrategy.kind() + " [" + name + "]" : unresolvedMessage;
    }

    @Override
    protected NodeInfo<UnresolvedFunction> info() {
        return NodeInfo.create(this, UnresolvedFunction::new,
            name, resolution, children(), analyzed, unresolvedMsg);
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new UnresolvedFunction(source(), name, resolution, newChildren, analyzed, unresolvedMsg);
    }

    public UnresolvedFunction withMessage(String message) {
        return new UnresolvedFunction(source(), name(), resolution, children(), true, message);
    }

    /**
     * Build a function to replace this one after resolving the function.
     */
    public Function buildResolved(Configuration configuration, FunctionDefinition def) {
        return resolution.buildResolved(this, configuration, def);
    }

    /**
     * Build a marker {@link UnresolvedFunction} with an error message
     * about the function being missing.
     */
    public UnresolvedFunction missing(String normalizedName, Iterable<FunctionDefinition> alternatives) {
        // try to find alternatives
        Set<String> names = new LinkedHashSet<>();
        for (FunctionDefinition def : alternatives) {
            if (resolution.isValidAlternative(def)) {
                names.add(def.name());
                names.addAll(def.aliases());
            }
        }

        List<String> matches = StringUtils.findSimilar(normalizedName, names);
        if (matches.isEmpty()) {
            return this;
        }
        String matchesMessage = matches.size() == 1 ? "[" + matches.get(0) + "]" : "any of " + matches;
        return withMessage("Unknown " + resolution.kind() + " [" + name + "], did you mean " + matchesMessage + "?");
    }

    @Override
    public boolean resolved() {
        return false;
    }

    public String name() {
        return name;
    }

    public FunctionResolutionStrategy resolutionStrategy() {
        return resolution;
    }

    public boolean analyzed() {
        return analyzed;
    }

    @Override
    public DataType dataType() {
        throw new UnresolvedException("dataType", this);
    }

    @Override
    public Nullability nullable() {
        throw new UnresolvedException("nullable", this);
    }

    @Override
    public ScriptTemplate asScript() {
        throw new UnresolvedException("script", this);
    }

    @Override
    public String unresolvedMessage() {
        return unresolvedMsg;
    }

    @Override
    public String toString() {
        return UNRESOLVED_PREFIX + name + children();
    }

    @Override
    public String nodeString() {
        return toString();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || obj.getClass() != getClass()) {
            return false;
        }
        UnresolvedFunction other = (UnresolvedFunction) obj;
        return name.equals(other.name)
            && resolution.equals(other.resolution)
            && children().equals(other.children())
            && analyzed == other.analyzed
            && Objects.equals(unresolvedMsg, other.unresolvedMsg);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, resolution, children(), analyzed, unresolvedMsg);
    }
}
