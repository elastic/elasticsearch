/// *
// * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// * or more contributor license agreements. Licensed under the "Elastic License
// * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
// * Public License v 1"; you may not use this file except in compliance with, at
// * your election, the "Elastic License 2.0", the "GNU Affero General Public
// * License v3.0 only", or the "Server Side Public License, v 1".
// */
//
// package org.elasticsearch.index.query;
//
// import org.elasticsearch.plugins.internal.rewriter.InternalQueryRewriter;
//
// import java.util.Map;
//
/// **
// * Enables modules and plugins to intercept and rewrite queries during the query rewrite phase on the coordinator node.
// */
// public interface QueryRewriteInterceptor extends InternalQueryRewriter {
//
// /**
// * Implementing classes should override this with the name of the query that is being
// * intercepted.
// */
// default String getName() {
// return null;
// }
//
// static QueryRewriteInterceptor multi(Map<String, QueryRewriteInterceptor> interceptors) {
// if (interceptors.isEmpty()) {
// return (context, queryBuilder) -> queryBuilder;
// }
// return new Multi(interceptors);
// }
//
// class Multi implements QueryRewriteInterceptor {
// final Map<String, QueryRewriteInterceptor> interceptors;
//
// private Multi(Map<String, QueryRewriteInterceptor> interceptors) {
// this.interceptors = interceptors;
// }
//
// @Override
// public QueryBuilder rewrite(QueryRewriteContext context, QueryBuilder queryBuilder) {
// QueryRewriteInterceptor interceptor = interceptors.get(queryBuilder.getName());
// if (interceptor != null) {
// return interceptor.rewrite(context, queryBuilder);
// }
// return queryBuilder;
// }
// }
// }
