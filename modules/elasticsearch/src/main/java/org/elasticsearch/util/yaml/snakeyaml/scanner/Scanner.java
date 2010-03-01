/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.util.yaml.snakeyaml.scanner;

import org.elasticsearch.util.yaml.snakeyaml.tokens.Token;

/**
 * This interface represents an input stream of {@link Token Tokens}.
 * <p>
 * The parser and the scanner form together the 'Parse' step in the loading
 * process (see chapter 3.1 of the <a href="http://yaml.org/spec/1.1/">YAML
 * Specification</a>).
 * </p>
 *
 * @see org.elasticsearch.util.yaml.snakeyaml.tokens.Token
 */
public interface Scanner {

    /**
     * Check if the next token is one of the given types.
     *
     * @param choices token IDs.
     * @return <code>true</code> if the next token can be assigned to a variable
     *         of at least one of the given types. Returns <code>false</code> if
     *         no more tokens are available.
     * @throws ScannerException Thrown in case of malformed input.
     */
    boolean checkToken(Token.ID... choices);

    /**
     * Return the next token, but do not delete it from the stream.
     *
     * @return The token that will be returned on the next call to
     *         {@link #getToken}
     * @throws ScannerException Thrown in case of malformed input.
     */
    Token peekToken();

    /**
     * Returns the next token.
     * <p>
     * The token will be removed from the stream.
     * </p>
     *
     * @throws ScannerException Thrown in case of malformed input.
     */
    Token getToken();
}
