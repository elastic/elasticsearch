/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.signature;

/**
 *
 */
public interface SignatureService {

    /**
     * Signs the given text and returns the signed text (original text + signature)
     */
    String sign(String text);

    /**
     * Unsigns the given signed text, verifies the original text with the attached signature and if valid returns
     * the unsigned (original) text. If signature verification fails a {@link SignatureException} is thrown.
     */
    String unsignAndVerify(String text);

    /**
     * Checks whether the given text is signed.
     */
    boolean signed(String text);

}
