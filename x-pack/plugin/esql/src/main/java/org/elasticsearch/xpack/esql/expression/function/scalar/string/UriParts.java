/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.string;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.compute.ann.Evaluator;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.function.scalar.EsqlScalarFunction;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.FIRST;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.SECOND;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isStringAndExact;
import static org.elasticsearch.xpack.esql.core.type.DataType.KEYWORD;

public class UriParts extends EsqlScalarFunction {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "UriParts", UriParts::new);

    private final Expression urlString;
    private final Expression field;

    @FunctionInfo(
        returnType = "keyword",
        description = "Parses a Uniform Resource Identifier (URI) string and extracts its component specified in the second argument.",
        examples = { @Example(file = "uri_parts", tag = "uri_parts") }
    )
    public UriParts(
        Source source,
        @Param(
            name = "urlString",
            type = { "keyword", "text" },
            description = "A Uniform Resource Identifier (URI) string."
        ) Expression urlString,
        @Param(name = "field", type = { "keyword", "text" }, description = "Component to extract.") Expression field
    ) {
        super(source, List.of(urlString, field));
        this.urlString = urlString;
        this.field = field;
    }

    private UriParts(StreamInput in) throws IOException {
        this(Source.readFrom((PlanStreamInput) in), in.readNamedWriteable(Expression.class), in.readNamedWriteable(Expression.class));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        source().writeTo(out);
        out.writeNamedWriteable(urlString);
        out.writeNamedWriteable(field);
    }

    @Override
    public boolean foldable() {
        return urlString.foldable() && field.foldable();
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    public DataType dataType() {
        return KEYWORD;
    }

    private static Map<String, Object> getUriParts(URI uri, URL fallbackUrl) {
        var uriParts = new HashMap<String, Object>();
        String domain;
        String fragment;
        String path;
        int port;
        String query;
        String scheme;
        String userInfo;

        if (uri != null) {
            domain = uri.getHost();
            fragment = uri.getFragment();
            path = uri.getPath();
            port = uri.getPort();
            query = uri.getQuery();
            scheme = uri.getScheme();
            userInfo = uri.getUserInfo();
        } else if (fallbackUrl != null) {
            domain = fallbackUrl.getHost();
            fragment = fallbackUrl.getRef();
            path = fallbackUrl.getPath();
            port = fallbackUrl.getPort();
            query = fallbackUrl.getQuery();
            scheme = fallbackUrl.getProtocol();
            userInfo = fallbackUrl.getUserInfo();
        } else {
            // should never occur during processor execution
            throw new IllegalArgumentException("at least one argument must be non-null");
        }

        uriParts.put("domain", domain);
        if (fragment != null) {
            uriParts.put("fragment", fragment);
        }
        if (path != null) {
            uriParts.put("path", path);
            // To avoid any issues with extracting the extension from a path that contains a dot, we explicitly extract the extension
            // from the last segment in the path.
            var lastSegmentIndex = path.lastIndexOf('/');
            if (lastSegmentIndex >= 0) {
                var lastSegment = path.substring(lastSegmentIndex);
                int periodIndex = lastSegment.lastIndexOf('.');
                if (periodIndex >= 0) {
                    // Don't include the dot in the extension field.
                    uriParts.put("extension", lastSegment.substring(periodIndex + 1));
                }
            }
        }
        if (port != -1) {
            uriParts.put("port", port);
        }
        if (query != null) {
            uriParts.put("query", query);
        }
        uriParts.put("scheme", scheme);
        if (userInfo != null) {
            uriParts.put("user_info", userInfo);
            if (userInfo.contains(":")) {
                int colonIndex = userInfo.indexOf(':');
                uriParts.put("username", userInfo.substring(0, colonIndex));
                uriParts.put("password", colonIndex < userInfo.length() ? userInfo.substring(colonIndex + 1) : "");
            }
        }

        return uriParts;
    }

    @Override
    protected TypeResolution resolveType() {
        if (childrenResolved() == false) {
            return new TypeResolution("Unresolved children");
        }

        TypeResolution resolution = isStringAndExact(urlString, sourceText(), FIRST);
        if (resolution.unresolved()) {
            return resolution;
        }

        return isStringAndExact(field, sourceText(), SECOND);
    }

    @Override
    public EvalOperator.ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        return new UriPartsEvaluator.Factory(source(), toEvaluator.apply(urlString), toEvaluator.apply(field));
    }

    @Evaluator(warnExceptions = { NullPointerException.class })
    static BytesRef process(BytesRef urlString, BytesRef field) {
        URI uri = null;
        URL url = null;

        try {
            uri = new URI(urlString.utf8ToString());
        } catch (URISyntaxException e) {
            try {
                url = new URL(urlString.utf8ToString());
            } catch (MalformedURLException e2) {
                throw new IllegalArgumentException("unable to parse URI [" + urlString.utf8ToString() + "]");
            }
        }

        return BytesRefs.toBytesRef(getUriParts(uri, url).get(field.utf8ToString()));
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new UriParts(source(), newChildren.get(0), newChildren.get(1));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, UriParts::new, urlString, field);
    }
}
