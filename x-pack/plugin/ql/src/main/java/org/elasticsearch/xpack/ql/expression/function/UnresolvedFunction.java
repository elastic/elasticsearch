/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ql.expression.function;

import org.elasticsearch.xpack.ql.capabilities.Unresolvable;
import org.elasticsearch.xpack.ql.capabilities.UnresolvedException;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.Literal;
import org.elasticsearch.xpack.ql.expression.Nullability;
import org.elasticsearch.xpack.ql.expression.function.aggregate.Count;
import org.elasticsearch.xpack.ql.expression.gen.script.ScriptTemplate;
import org.elasticsearch.xpack.ql.session.Configuration;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.util.StringUtils;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;

import static java.util.Collections.singletonList;

public class UnresolvedFunction extends Function implements Unresolvable {
    private final String name;
    private final String unresolvedMsg;
    /**
     * How the resolution should be performed. This is changed depending
     * on how the function was called.
     */
    private final ResolutionType resolutionType;
    /**
     * Flag to indicate analysis has been applied and there's no point in
     * doing it again this is an optimization to prevent searching for a
     * better unresolved message over and over again.
     */
    private final boolean analyzed;

    public UnresolvedFunction(Source source, String name, ResolutionType resolutionType, List<Expression> children) {
        this(source, name, resolutionType, children, false, null);
    }

    /**
     * Constructor used for specifying a more descriptive message (typically
     * 'did you mean') instead of the default one.
     * @see #withMessage(String)
     */
    UnresolvedFunction(Source source, String name, ResolutionType resolutionType, List<Expression> children,
            boolean analyzed, String unresolvedMessage) {
        super(source, children);
        this.name = name;
        this.resolutionType = resolutionType;
        this.analyzed = analyzed;
        this.unresolvedMsg = unresolvedMessage == null ? "Unknown " + resolutionType.type() + " [" + name + "]" : unresolvedMessage;
    }

    @Override
    protected NodeInfo<UnresolvedFunction> info() {
        return NodeInfo.create(this, UnresolvedFunction::new,
            name, resolutionType, children(), analyzed, unresolvedMsg);
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new UnresolvedFunction(source(), name, resolutionType, newChildren, analyzed, unresolvedMsg);
    }

    public UnresolvedFunction withMessage(String message) {
        return new UnresolvedFunction(source(), name(), resolutionType, children(), true, message);
    }

    public UnresolvedFunction preprocessStar() {
        return resolutionType.preprocessStar(this);
    }

    /**
     * Build a function to replace this one after resolving the function.
     */
    public Function buildResolved(Configuration configuration, FunctionDefinition def) {
        return resolutionType.buildResolved(this, configuration, def);
    }

    /**
     * Build a marker {@link UnresolvedFunction} with an error message
     * about the function being missing.
     */
    public UnresolvedFunction missing(String normalizedName, Iterable<FunctionDefinition> alternatives) {
        // try to find alternatives
        Set<String> names = new LinkedHashSet<>();
        for (FunctionDefinition def : alternatives) {
            if (resolutionType.isValidAlternative(def)) {
                names.add(def.name());
                names.addAll(def.aliases());
            }
        }

        List<String> matches = StringUtils.findSimilar(normalizedName, names);
        if (matches.isEmpty()) {
            return this;
        }
        String matchesMessage = matches.size() == 1 ? "[" + matches.get(0) + "]" : "any of " + matches;
        return withMessage("Unknown " + resolutionType.type() + " [" + name + "], did you mean " + matchesMessage + "?");
    }

    @Override
    public boolean resolved() {
        return false;
    }

    public String name() {
        return name;
    }

    ResolutionType resolutionType() {
        return resolutionType;
    }

    public boolean analyzed() {
        return analyzed;
    }
    
    public boolean sameAs(Count count) {
        if (this.resolutionType == ResolutionType.DISTINCT && count.distinct()
                || this.resolutionType == ResolutionType.STANDARD && count.distinct() == false) {
            return true;
        }
        return false;
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
            && resolutionType.equals(other.resolutionType)
            && children().equals(other.children())
            && analyzed == other.analyzed
            && Objects.equals(unresolvedMsg, other.unresolvedMsg);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, resolutionType, children(), analyzed, unresolvedMsg);
    }

    /**
     * Customize how function resolution works based on
     * where the function appeared in the grammar.
     */
    public enum ResolutionType {
        /**
         * Behavior of standard function calls like {@code ABS(col)}.
         */
        STANDARD {
            @Override
            public UnresolvedFunction preprocessStar(UnresolvedFunction uf) {
                // TODO: might be removed
                // dedicated count optimization
                if (uf.name.toUpperCase(Locale.ROOT).equals("COUNT")) {
                    return new UnresolvedFunction(uf.source(), uf.name(), uf.resolutionType,
                            singletonList(new Literal(uf.arguments().get(0).source(), Integer.valueOf(1), DataType.INTEGER)));
                }
                return uf;
            }
            @Override
            public Function buildResolved(UnresolvedFunction uf, Configuration cfg, FunctionDefinition def) {
                return def.builder().build(uf, false, cfg);
            }
            @Override
            protected boolean isValidAlternative(FunctionDefinition def) {
                return true;
            }
            @Override
            protected String type() {
                return "function";
            }
        },
        /**
         * Behavior of DISTINCT like {@code COUNT DISTINCT(col)}.
         */
        DISTINCT {
            @Override
            public UnresolvedFunction preprocessStar(UnresolvedFunction uf) {
                return uf.withMessage("* is not valid with DISTINCT");
            }
            @Override
            public Function buildResolved(UnresolvedFunction uf, Configuration cfg, FunctionDefinition def) {
                return def.builder().build(uf, true, cfg);
            }
            @Override
            protected boolean isValidAlternative(FunctionDefinition def) {
                return false; // think about this later.
            }
            @Override
            protected String type() {
                return "function";
            }
        },
        /**
         * Behavior of EXTRACT function calls like {@code EXTRACT(DAY FROM col)}.
         */
        EXTRACT {
            @Override
            public UnresolvedFunction preprocessStar(UnresolvedFunction uf) {
                return uf.withMessage("Can't extract from *");
            }
            @Override
            public Function buildResolved(UnresolvedFunction uf, Configuration cfg, FunctionDefinition def) {
                if (def.extractViable()) {
                    return def.builder().build(uf, false, cfg);
                }
                return uf.withMessage("Invalid datetime field [" + uf.name() + "]. Use any datetime function.");
            }
            @Override
            protected boolean isValidAlternative(FunctionDefinition def) {
                return def.extractViable();
            }
            @Override
            protected String type() {
                return "datetime field";
            }
        };
        /**
         * Preprocess a function that contains a star to some other
         * form before attempting to resolve it. For example,
         * {@code DISTINCT} doesn't support {@code *} so it converts
         * this function into a dead end, unresolveable function.
         * Or {@code COUNT(*)} can be rewritten to {@code COUNT(1)}
         * so we don't have to resolve {@code *}.
         */
        protected abstract UnresolvedFunction preprocessStar(UnresolvedFunction uf);
        /**
         * Build the real function from this one and resolution metadata.
         */
        protected abstract Function buildResolved(UnresolvedFunction uf, Configuration cfg, FunctionDefinition def);
        /**
         * Is {@code def} a valid alternative for function invocations
         * of this kind. Used to filter the list of "did you mean"
         * options sent back to the user when they specify a missing
         * function.
         */
        protected abstract boolean isValidAlternative(FunctionDefinition def);
        /**
         * The name of the kind of thing being resolved. Used when
         * building the error message sent back to the user when
         * they specify a function that doesn't exist.
         */
        protected abstract String type();
    }
}
