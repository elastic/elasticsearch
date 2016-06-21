/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authc.support;

import org.elasticsearch.test.ESTestCase;

import java.nio.charset.StandardCharsets;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;

public class SecuredStringTests extends ESTestCase {
    public static SecuredString build(String password){
        return new SecuredString(password.toCharArray());
    }

    public void testAccessAfterClear(){
        SecuredString password = new SecuredString("password".toCharArray());
        SecuredString password2 = new SecuredString("password".toCharArray());

        password.clear();

        try {
            password.internalChars();
            fail();
        } catch(Exception e){}

        try {
            password.length();
            fail();
        } catch(Exception e){}

        try {
            password.charAt(0);
            fail();
        } catch(Exception e){}

        try {
            password.concat("_suffix");
            fail();
        } catch(Exception e){}

        assertNotEquals(password, password2);
    }

    public void testEqualsHashCode(){
        SecuredString password = new SecuredString("password".toCharArray());
        SecuredString password2 = new SecuredString("password".toCharArray());

        assertEquals(password, password2);
        assertEquals(password.hashCode(), password2.hashCode());
    }

    public void testsEqualsCharSequence(){
        SecuredString password = new SecuredString("password".toCharArray());
        StringBuffer password2 = new StringBuffer("password");
        String password3 = "password";

        assertEquals(password, password2);
        assertEquals(password, password3);
    }

    public void testConcat() {
        SecuredString password = new SecuredString("password".toCharArray());
        SecuredString password2 = new SecuredString("password".toCharArray());

        SecuredString password3 = password.concat(password2);
        assertThat(password3.length(), equalTo(password.length() + password2.length()));
        assertThat(password3.internalChars(), equalTo("passwordpassword".toCharArray()));
    }

    public void testSubsequence(){
        SecuredString password = new SecuredString("password".toCharArray());
        SecuredString password2 = password.subSequence(4, 8);
        SecuredString password3 = password.subSequence(0, 4);

        assertThat(password2.internalChars(), equalTo("word".toCharArray()));
        assertThat(password3.internalChars(), equalTo("pass".toCharArray()));
        assertThat("ensure original is unmodified", password.internalChars(), equalTo("password".toCharArray()));
    }

    public void testUFT8(){
        String password = "эластичный поиск-弾性検索";
        SecuredString securePass = new SecuredString(password.toCharArray());
        byte[] utf8 = securePass.utf8Bytes();
        String password2 = new String(utf8, StandardCharsets.UTF_8);
        assertThat(password2, equalTo(password));
    }

    public void testCopyChars() throws Exception {
        String password = "эластичный поиск-弾性検索";
        SecuredString securePass = new SecuredString(password.toCharArray());
        char[] copy = securePass.copyChars();
        assertThat(copy, not(sameInstance(securePass.internalChars())));
        assertThat(copy, equalTo(securePass.internalChars()));

        // just a sanity check to make sure that clearing the secured string
        // doesn't modify the returned copied chars
        securePass.clear();
        assertThat(new String(copy), equalTo("эластичный поиск-弾性検索"));
    }
}
