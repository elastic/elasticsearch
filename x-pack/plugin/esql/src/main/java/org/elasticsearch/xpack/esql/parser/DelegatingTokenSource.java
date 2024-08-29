/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.parser;

import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.TokenFactory;
import org.antlr.v4.runtime.TokenSource;

/**
 * Utility class for filtering/processing TokenSource.
 */
abstract class DelegatingTokenSource implements TokenSource {
    final TokenSource delegate;

    DelegatingTokenSource(TokenSource delegate) {
        this.delegate = delegate;
    }

    @Override
    public Token nextToken() {
        return delegate.nextToken();
    }

    @Override
    public int getLine() {
        return delegate.getLine();
    }

    @Override
    public int getCharPositionInLine() {
        return delegate.getCharPositionInLine();
    }

    @Override
    public CharStream getInputStream() {
        return delegate.getInputStream();
    }

    @Override
    public String getSourceName() {
        return delegate.getSourceName();
    }

    @Override
    public void setTokenFactory(TokenFactory<?> tokenFactory) {
        delegate.setTokenFactory(tokenFactory);
    }

    @Override
    public TokenFactory<?> getTokenFactory() {
        return delegate.getTokenFactory();
    }
}
