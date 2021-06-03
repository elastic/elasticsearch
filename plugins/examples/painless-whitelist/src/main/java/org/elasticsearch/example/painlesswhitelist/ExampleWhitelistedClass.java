/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.example.painlesswhitelist;

/**
 * An example of a class to be whitelisted for use by painless scripts
 *
 * Each of the members and methods below are whitelisted for use in search scripts.
 * See <a href="file:example_whitelist.txt">example_whitelist.txt</a>.
 */
public class ExampleWhitelistedClass {

    public static final int CONSTANT = 42;

    public int publicMember;

    private int privateMember;

    public ExampleWhitelistedClass(int publicMember, int privateMember) {
        this.publicMember = publicMember;
        this.privateMember = privateMember;
    }

    public int getPrivateMemberAccessor() {
        return this.privateMember;
    }

    public void setPrivateMemberAccessor(int privateMember) {
        this.privateMember = privateMember;
    }

    public static void staticMethod() {
        // electricity
    }

    // example augmentation method
    public static int toInt(String x) {
        return Integer.parseInt(x);
    }

    // example method to attach annotations in whitelist
    public void annotate() {
        // some logic here
    }
}
