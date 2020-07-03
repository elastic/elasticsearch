/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.wildcard.mapper;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;

import java.io.IOException;

/**
 * A query that matches all documents. The class is more of a marker
 * that we encountered something that will need verification.
 * (A MatchAllDocs query is used to indicate we can match all
 * _without_ verification)
 */
public final class MatchAllButRequireVerificationQuery extends Query {

  @Override
    public Query rewrite(IndexReader reader) throws IOException {
        return new MatchAllDocsQuery();
    }

  @Override
  public String toString(String field) {
    return "*:* (tbc)";
  }

  @Override
  public boolean equals(Object o) {
    return sameClassAs(o);
  }

  @Override
  public int hashCode() {
    return classHash();
  }

  @Override
  public void visit(QueryVisitor visitor) {
    visitor.visitLeaf(this);
  }

    
}
