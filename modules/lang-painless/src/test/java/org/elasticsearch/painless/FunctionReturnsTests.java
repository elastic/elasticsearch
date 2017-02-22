/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

package org.elasticsearch.painless;

/**
 * Tests casting behavior around function return values. This amounts to tests of implicit casting.
 */
public class FunctionReturnsTests extends ScriptTestCase {
    public void testReturnsAreUnboxedIfNeeded() {
        assertEquals((byte) 5, exec(   "byte get() {Byte.valueOf(5)} get()"));
        assertEquals((short) 5, exec( "short get() {Byte.valueOf(5)} get()"));
        assertEquals(5, exec(           "int get() {Byte.valueOf(5)} get()"));
        assertEquals((short) 5, exec( "short get() {Short.valueOf(5)} get()"));
        assertEquals(5, exec(           "int get() {Integer.valueOf(5)} get()"));
        assertEquals(5.0f, exec(      "float get() {Float.valueOf(5)} get()"));
        assertEquals(5.0d, exec(     "double get() {Float.valueOf(5)} get()"));
        assertEquals(5.0d, exec(     "double get() {Double.valueOf(5)} get()"));
        assertEquals(true, exec(    "boolean get() {Boolean.TRUE} get()"));
    }

    public void testReturnByte() {
        returnByteOk("byte");
    }

    public void testReturnShort() {
        returnShortOk("short");
    }

    public void testReturnChar() {
        returnIntOk("char"); // NOCOMMIT return char ok?
    }

    public void testReturnInt() {
        returnIntOk("int");
    }

    public void testReturnLong() {
        returnLongOk("long");
    }

    public void testReturnFloat() {
        returnFloatOk("float");
    }

    public void testReturnDouble() {
        returnDoubleOk("double");
    }

    private void returnByteOk(String type) {
        assertEquals((byte) 5, exec(     "byte get() {(" + type + ") 5} get()"));
        assertEquals((byte) 5, exec(     "Byte get() {(" + type + ") 5} get()"));
        assertEquals((byte) 5, exec(     "byte get(" + type + " b) {        b} get(5)"));
        assertEquals((byte) 5, exec(     "Byte get(" + type + " b) {        b} get(5)"));
        returnShortOk(type);
    }

    private void returnShortOk(String type) {
        assertEquals((short) 5, exec(   "short get() {(" + type + ") 5} get()"));
        assertEquals((short) 5, exec(   "Short get() {(" + type + ") 5} get()"));
        assertEquals((short) 5, exec(   "short get(" + type + " b) {        b} get(5)"));
        assertEquals((short) 5, exec(   "Short get(" + type + " b) {        b} get(5)"));
        returnIntOk(type);
    }

    private void returnIntOk(String type) {
        assertEquals(5, exec(             "int get() {(" + type + ") 5} get()"));
        assertEquals(5, exec(         "Integer get() {(" + type + ") 5} get()"));
        assertEquals(5, exec(             "int get(" + type + " b) {        b} get(5)"));
        assertEquals(5, exec(         "Integer get(" + type + " b) {        b} get(5)"));
        returnLongOk(type);
    }

    private void returnLongOk(String type) {
        assertEquals(5L, exec(           "long get() {(" + type + ") 5} get()"));
        assertEquals(5L, exec(           "Long get() {(" + type + ") 5} get()"));
        assertEquals(5L, exec(           "long get(" + type + " b) {        b} get(5)"));
        assertEquals(5L, exec(           "Long get(" + type + " b) {        b} get(5)"));
        returnFloatOk(type);
    }

    private void returnFloatOk(String type) {
        assertEquals(5f, exec(          "float get() {(" + type + ") 5} get()"));
        assertEquals(5f, exec(          "Float get() {(" + type + ") 5} get()"));
        assertEquals(5f, exec(          "float get(" + type + " b) {        b} get(5)"));
        assertEquals(5f, exec(          "Float get(" + type + " b) {        b} get(5)"));
        returnDoubleOk(type);
    }

    private void returnDoubleOk(String type) {
        assertEquals(5d, exec(         "double get() {(" + type + ") 5} get()"));
        assertEquals(5d, exec(         "Double get() {(" + type + ") 5} get()"));

        assertEquals((byte) 5, exec(     "byte get(" + type + " b) { (byte) b} get(5)"));
        assertEquals((short) 5, exec(   "short get(" + type + " b) {(short) b} get(5)"));
        assertEquals((char) 5, exec(     "char get(" + type + " b) { (char) b} get(5)"));
        assertEquals(5, exec(             "int get(" + type + " b) {  (int) b} get(5)"));
        assertEquals(5L, exec(           "long get(" + type + " b) { (long) b} get(5)"));
        assertEquals(5f, exec(          "float get(" + type + " b) {(float) b} get(5)"));
        assertEquals(5d, exec(         "double get(" + type + " b) {        b} get(5)"));
        assertEquals((byte) 5, exec(     "Byte get(" + type + " b) { (byte) b} get(5)"));
        assertEquals((short) 5, exec(   "Short get(" + type + " b) {(short) b} get(5)"));
        assertEquals((char) 5, exec("Character get(" + type + " b) { (char) b} get(5)"));
        assertEquals(5, exec(         "Integer get(" + type + " b) {  (int) b} get(5)"));
        assertEquals(5L, exec(           "Long get(" + type + " b) { (long) b} get(5)"));
        assertEquals(5f, exec(          "Float get(" + type + " b) {(float) b} get(5)"));
        assertEquals(5d, exec(         "Double get(" + type + " b) {        b} get(5)"));
        assertEquals(5, ((Number)exec( "Number get(" + type + " b) {        b} get(5)")).intValue());
    }
}
