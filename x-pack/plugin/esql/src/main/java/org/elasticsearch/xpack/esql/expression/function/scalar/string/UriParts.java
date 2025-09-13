/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.string;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.ColumnsBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.compute.operator.Warnings;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.TypeResolutions;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.function.scalar.UnaryScalarFunction;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isString;

public class UriParts extends UnaryScalarFunction {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "Trim", UriParts::new);

    @FunctionInfo(
        returnType = { "columns" },
        description = "NOCOMMIT",
        examples = {} // NOCOMMIT
    )
    public UriParts(
        Source source,
        @Param(
            name = "string",
            type = { "keyword", "text" },
            description = "URI/URL to parse. If `null`, the function returns `null`."
        ) Expression str
    ) {
        super(source, str);
    }

    private UriParts(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    protected TypeResolution resolveType() {
        if (childrenResolved() == false) {
            return new Expression.TypeResolution("Unresolved children");
        }

        return isString(field, sourceText(), TypeResolutions.ParamOrdinal.DEFAULT);
    }

    @Override
    public DataType dataType() {
        return DataType.COLUMNS;
    }

    @Override
    public EvalOperator.ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        var field = toEvaluator.apply(field());
        return new EvaluatorFactory(source(), field);
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new UriParts(source(), newChildren.getFirst());
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, UriParts::new, field());
    }

    static class EvaluatorFactory implements EvalOperator.ExpressionEvaluator.Factory {
        private final Source source;
        private final EvalOperator.ExpressionEvaluator.Factory val;

        EvaluatorFactory(Source source, EvalOperator.ExpressionEvaluator.Factory val) {
            this.source = source;
            this.val = val;
        }

        @Override
        public Evaluator get(DriverContext context) {
            return new Evaluator(source, val.get(context), context);
        }

        @Override
        public String toString() {
            return "UriPartsEvaluator[val=" + val + "]";
        }
    }

    static class Evaluator implements EvalOperator.ExpressionEvaluator {
        private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(Evaluator.class);

        private final Source source;

        private final EvalOperator.ExpressionEvaluator val;

        private final DriverContext driverContext;

        private Warnings warnings;

        Evaluator(Source source, EvalOperator.ExpressionEvaluator val, DriverContext driverContext) {
            this.source = source;
            this.val = val;
            this.driverContext = driverContext;
        }

        @Override
        public Block eval(Page page) {
            try (BytesRefBlock valBlock = (BytesRefBlock) val.eval(page)) {
                BytesRefVector valVector = valBlock.asVector();
                if (valVector == null) {
                    throw new UnsupportedOperationException("NOCOMMIT TODO UriParts");
                }
                return new Process(valVector.getPositionCount(), driverContext.blockFactory()).eval(valVector);
            }
        }

        @Override
        public long baseRamBytesUsed() {
            long baseRamBytesUsed = BASE_RAM_BYTES_USED;
            baseRamBytesUsed += val.baseRamBytesUsed();
            return baseRamBytesUsed;
        }

        Warnings warnings() {
            if (warnings == null) {
                this.warnings = Warnings.createWarnings(
                    driverContext.warningsMode(),
                    source.source().getLineNumber(),
                    source.source().getColumnNumber(),
                    source.text()
                );
            }
            return warnings;
        }

        @Override
        public void close() {
            Releasables.close(val);
        }

        @Override
        public String toString() {
            return "NOCOMMIT";
        }

        class Process {
            private final BytesRefBlock.Builder scheme;
            private final BytesRefBlock.Builder userInfo;
            private final BytesRefBlock.Builder domain;
            private final IntBlock.Builder port;
            private final BytesRefBlock.Builder path;
            private final BytesRefBlock.Builder extension;
            private final BytesRefBlock.Builder query;
            private final BytesRefBlock.Builder fragment;

            private final BytesRefBuilder scratch;

            Process(int estimatedSize, BlockFactory factory) {
                BytesRefBlock.Builder scheme = null;
                BytesRefBlock.Builder userInfo = null;
                BytesRefBlock.Builder domain = null;
                IntBlock.Builder port = null;
                BytesRefBlock.Builder path = null;
                BytesRefBlock.Builder extension = null;
                BytesRefBlock.Builder query = null;
                BytesRefBlock.Builder fragment = null;

                try {
                    scheme = factory.newBytesRefBlockBuilder(estimatedSize);
                    userInfo = factory.newBytesRefBlockBuilder(estimatedSize);
                    domain = factory.newBytesRefBlockBuilder(estimatedSize);
                    port = factory.newIntBlockBuilder(estimatedSize);
                    path = factory.newBytesRefBlockBuilder(estimatedSize);
                    extension = factory.newBytesRefBlockBuilder(estimatedSize);
                    query = factory.newBytesRefBlockBuilder(estimatedSize);
                    fragment = factory.newBytesRefBlockBuilder(estimatedSize);

                    this.scheme = scheme;
                    this.userInfo = userInfo;
                    this.domain = domain;
                    this.port = port;
                    this.path = path;
                    this.extension = extension;
                    this.query = query;
                    this.fragment = fragment;
                } finally {
                    // NOCOMMIT this needs to be the last one
                    if (extension == null) {
                        // NOCOMMIT this needs to be all of them
                        Releasables.close(scheme, userInfo, domain, port, path, extension, query, fragment);
                    }
                }
                scratch = new BytesRefBuilder();
            }

            public Block eval(BytesRefVector valVector) {
                for (int i = 0; i < valVector.getPositionCount(); i++) {
                    BytesRef v = valVector.getBytesRef(i, scratch.get());
                    apply(v.utf8ToString());
                }
                return finish();
            }

            private Block finish() {
                BytesRefBlock scheme = null;
                BytesRefBlock userInfo = null;
                BytesRefBlock domain = null;
                IntBlock port = null;
                BytesRefBlock path = null;
                BytesRefBlock extension = null;
                BytesRefBlock query = null;
                BytesRefBlock fragment = null;
                ColumnsBlock result = null;
                try {
                    scheme = this.scheme.build();
                    userInfo = this.userInfo.build();
                    domain = this.domain.build();
                    port = this.port.build();
                    path = this.path.build();
                    extension = this.extension.build();
                    query = this.query.build();
                    fragment = this.fragment.build();

                    result = new ColumnsBlock(
                        domain.blockFactory(),
                        domain.getPositionCount(),
                        Map.ofEntries(
                            Map.entry("scheme", new ColumnsBlock.RuntimeTypedBlock(DataType.KEYWORD, scheme)),
                            Map.entry("user_info", new ColumnsBlock.RuntimeTypedBlock(DataType.KEYWORD, userInfo)),
                            Map.entry("domain", new ColumnsBlock.RuntimeTypedBlock(DataType.KEYWORD, domain)),
                            Map.entry("port", new ColumnsBlock.RuntimeTypedBlock(DataType.INTEGER, port)),
                            Map.entry("path", new ColumnsBlock.RuntimeTypedBlock(DataType.KEYWORD, path)),
                            Map.entry("extension", new ColumnsBlock.RuntimeTypedBlock(DataType.KEYWORD, extension)),
                            Map.entry("query", new ColumnsBlock.RuntimeTypedBlock(DataType.KEYWORD, query)),
                            Map.entry("fragment", new ColumnsBlock.RuntimeTypedBlock(DataType.KEYWORD, fragment))
                        )
                    );
                    return result;
                } finally {
                    if (result == null) {
                        Releasables.close(scheme, userInfo, domain, port, path, extension, query, fragment);
                    }
                }
            }

            public void apply(String urlString) {
                try {
                    apply(new URI(urlString));
                } catch (URISyntaxException e) {
                    try {
                        apply(new URL(urlString));
                    } catch (MalformedURLException e2) {
                        unableToParse(urlString);
                    }
                }
            }

            private void apply(URI uri) {
                apply(uri.getScheme(), uri.getUserInfo(), uri.getHost(), uri.getPort(), uri.getPath(), uri.getQuery(), uri.getFragment());
            }

            @SuppressForbidden(reason = "URL.getPath is used only if URI.getPath is unavailable")
            private void apply(URL url) {
                apply(url.getProtocol(), url.getUserInfo(), url.getHost(), url.getPort(), url.getPath(), url.getQuery(), url.getRef());
            }

            private void unableToParse(String urlString) {
                scheme.appendNull();
                userInfo.appendNull();
                domain.appendNull();
                port.appendNull();
                path.appendNull();
                extension.appendNull();
                query.appendNull();
                fragment.appendNull();
                warnings().registerException(IllegalArgumentException.class, "unable to parse URI [" + urlString + "]");
            }

            private void apply(String scheme, String userInfo, String domain, int port, String path, String query, String fragment) {
                append(this.scheme, scheme);
                if (userInfo == null) {
                    this.userInfo.appendNull();
                } else {
                    append(this.userInfo, userInfo);
                }
                if (domain == null) {
                    this.domain.appendNull();
                } else {
                    append(this.domain, domain);
                }
                if (port < 0) {
                    this.port.appendNull();
                } else {
                    this.port.appendInt(port);
                }
                if (path == null) {
                    this.path.appendNull();
                    this.extension.appendNull();
                } else {
                    appendPath(path);
                }
                if (query == null) {
                    this.query.appendNull();
                } else {
                    append(this.query, query);
                }
                if (fragment == null) {
                    this.fragment.appendNull();
                } else {
                    append(this.fragment, query);
                }
            }

            private void append(BytesRefBlock.Builder builder, String value) {
                scratch.clear();
                scratch.copyChars(value);
                builder.appendBytesRef(scratch.get());
            }

            private void appendPath(String path) {
                append(this.path, path);
                int lastSegmentIndex = path.lastIndexOf('/');
                if (lastSegmentIndex < 0) {
                    this.extension.appendNull();
                    return;
                }
                String lastSegment = path.substring(lastSegmentIndex);
                int periodIndex = path.lastIndexOf('.');
                if (periodIndex < 0) {
                    this.extension.appendNull();
                    return;
                }
                append(this.extension, lastSegment.substring(periodIndex + 1));
            }
        }
    }
}
