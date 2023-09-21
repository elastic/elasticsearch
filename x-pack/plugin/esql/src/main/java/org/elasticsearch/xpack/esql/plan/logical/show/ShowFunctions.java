/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.show;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.Strings;
import org.elasticsearch.xpack.esql.expression.function.Described;
import org.elasticsearch.xpack.esql.expression.function.Named;
import org.elasticsearch.xpack.esql.expression.function.Optional;
import org.elasticsearch.xpack.esql.expression.function.Typed;
import org.elasticsearch.xpack.ql.expression.Attribute;
import org.elasticsearch.xpack.ql.expression.ReferenceAttribute;
import org.elasticsearch.xpack.ql.expression.function.FunctionDefinition;
import org.elasticsearch.xpack.ql.expression.function.FunctionRegistry;
import org.elasticsearch.xpack.ql.plan.logical.LeafPlan;
import org.elasticsearch.xpack.ql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.ql.session.Configuration;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.ql.type.DataTypes.BOOLEAN;
import static org.elasticsearch.xpack.ql.type.DataTypes.KEYWORD;

public class ShowFunctions extends LeafPlan {

    private final List<Attribute> attributes;

    public record ArgSignature(String name, String type, String description, boolean optional) {}

    public record FunctionSignature(String name, List<ArgSignature> args, String returnType, String description, boolean variadic) {
        public String fullSignature() {
            StringBuilder builder = new StringBuilder();
            builder.append(returnType);
            builder.append(" ");
            builder.append(name);
            builder.append("(");
            for (int i = 0; i < args.size(); i++) {
                ArgSignature arg = args.get(i);
                if (i > 0) {
                    builder.append(", ");
                }
                if (arg.optional()) {
                    builder.append("?");
                }
                builder.append(arg.name());
                if (i == args.size() - 1 && variadic) {
                    builder.append("...");
                }
                builder.append(":");
                builder.append(arg.type());
            }
            builder.append(")");
            return builder.toString();
        }

        public List<String> argNames() {
            return args.stream().map(ArgSignature::name).collect(Collectors.toList());
        }

    }

    public ShowFunctions(Source source) {
        super(source);

        attributes = new ArrayList<>();
        for (var name : List.of("name", "synopsis", "argNames", "argTypes", "argDescriptions", "returnType", "description")) {
            attributes.add(new ReferenceAttribute(Source.EMPTY, name, KEYWORD));
        }
        for (var name : List.of("optionalArgs", "variadic")) {
            attributes.add(new ReferenceAttribute(Source.EMPTY, name, BOOLEAN));
        }
    }

    @Override
    public List<Attribute> output() {
        return attributes;
    }

    public List<List<Object>> values(FunctionRegistry functionRegistry) {
        List<List<Object>> rows = new ArrayList<>();
        for (var def : functionRegistry.listFunctions(null)) {
            FunctionSignature signature = signature(def);
            List<Object> row = new ArrayList<>();
            row.add(asBytesRefOrNull(signature.name()));
            row.add(new BytesRef(signature.fullSignature()));
            row.add(getCollect(signature, ArgSignature::name));
            row.add(getCollect(signature, ArgSignature::type));
            row.add(getCollect(signature, ArgSignature::description));
            row.add(signature.returnType);
            row.add(signature.description);
            row.add(getCollect(signature, ArgSignature::optional));
            row.add(signature.variadic);
            rows.add(row);
        }
        rows.sort(Comparator.comparing(x -> ((BytesRef) x.get(0))));
        return rows;
    }

    private Object getCollect(FunctionSignature signature, Function<ArgSignature, ?> x) {
        if (signature.args.size() == 0) {
            return null;
        }
        if (signature.args.size() == 1) {
            return x.apply(signature.args.get(0));
        }
        return signature.args().stream().map(x).collect(Collectors.toList());
    }

    /**
     * Returns information about a function signature
     */
    public static FunctionSignature signature(FunctionDefinition def) {
        var constructors = def.clazz().getConstructors();
        if (constructors.length == 0) {
            return new FunctionSignature(def.name(), List.of(), null, null, false);
        }
        Constructor<?> constructor = constructors[0];
        Described functionDescAnn = constructor.getAnnotation(Described.class);
        String functionDescription = functionDescAnn == null ? "" : functionDescAnn.value();
        Typed returnTypeAnn = constructor.getAnnotation(Typed.class);
        String returnType = returnTypeAnn == null ? "?" : returnTypeAnn.value();
        var params = constructor.getParameters(); // no multiple c'tors supported

        List<ArgSignature> args = new ArrayList<>(params.length);
        boolean variadic = false;
        for (int i = 1; i < params.length; i++) { // skipping 1st argument, the source
            if (Configuration.class.isAssignableFrom(params[i].getType()) == false) {
                Named namedAnn = params[i].getAnnotation(Named.class);
                String name = namedAnn == null ? params[i].getName() : namedAnn.value();

                if (List.class.isAssignableFrom(params[i].getType())) {
                    variadic = true;
                }

                Typed typedAnn = params[i].getAnnotation(Typed.class);
                String type = typedAnn == null ? "?" : typedAnn.value();

                Described describedAnn = params[i].getAnnotation(Described.class);
                String desc = describedAnn == null ? "" : describedAnn.value();

                boolean optional = params[i].getAnnotation(Optional.class) != null;

                args.add(new ArgSignature(name, type, desc, optional));
            }
        }
        return new FunctionSignature(def.name(), args, returnType, functionDescription, variadic);
    }

    private static BytesRef asBytesRefOrNull(String string) {
        return Strings.hasText(string) ? new BytesRef(string) : null;
    }

    @Override
    public boolean expressionsResolved() {
        return true;
    }

    @Override
    protected NodeInfo<? extends LogicalPlan> info() {
        return NodeInfo.create(this);
    }

    @Override
    public int hashCode() {
        return getClass().hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        return this == obj || obj != null && getClass() == obj.getClass();
    }
}
