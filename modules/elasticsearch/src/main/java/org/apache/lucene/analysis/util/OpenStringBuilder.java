package org.apache.lucene.analysis.util;

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * A StringBuilder that allows one to access the array.
 */
// LUCENE MONITOR: Remove as of Lucene 3.3
public class OpenStringBuilder implements Appendable, CharSequence {
  protected char[] buf;
  protected int len;

  public OpenStringBuilder() {
    this(32);
  }

  public OpenStringBuilder(int size) {
    buf = new char[size];
  }

  public OpenStringBuilder(char[] arr, int len) {
    set(arr, len);
  }

  public void setLength(int len) { this.len = len; }

  public void set(char[] arr, int end) {
    this.buf = arr;
    this.len = end;
  }

  public char[] getArray() { return buf; }
  public int size() { return len; }
  public int length() { return len; }
  public int capacity() { return buf.length; }

  public Appendable append(CharSequence csq) {
    return append(csq, 0, csq.length());
  }

  public Appendable append(CharSequence csq, int start, int end) {
    reserve(end-start);
    for (int i=start; i<end; i++) {
      unsafeWrite(csq.charAt(i));
    }
    return this;
  }

  public Appendable append(char c) {
    write(c);
    return this;
  }

  public char charAt(int index) {
    return buf[index];
  }

  public void setCharAt(int index, char ch) {
    buf[index] = ch;    
  }

  public CharSequence subSequence(int start, int end) {
    throw new UnsupportedOperationException(); // todo
  }

  public void unsafeWrite(char b) {
    buf[len++] = b;
  }

  public void unsafeWrite(int b) { unsafeWrite((char)b); }

  public void unsafeWrite(char b[], int off, int len) {
    System.arraycopy(b, off, buf, this.len, len);
    this.len += len;
  }

  protected void resize(int len) {
    char newbuf[] = new char[Math.max(buf.length << 1, len)];
    System.arraycopy(buf, 0, newbuf, 0, size());
    buf = newbuf;
  }

  public void reserve(int num) {
    if (len + num > buf.length) resize(len + num);
  }

  public void write(char b) {
    if (len >= buf.length) {
      resize(len +1);
    }
    unsafeWrite(b);
  }

  public void write(int b) { write((char)b); }

  public final void write(char[] b) {
    write(b,0,b.length);
  }

  public void write(char b[], int off, int len) {
    reserve(len);
    unsafeWrite(b, off, len);
  }

  public final void write(OpenStringBuilder arr) {
    write(arr.buf, 0, len);
  }

  public void write(String s) {
    reserve(s.length());
    s.getChars(0,s.length(),buf, len);
    len +=s.length();
  }

  public void flush() {
  }

  public final void reset() {
    len =0;
  }

  public char[] toCharArray() {
    char newbuf[] = new char[size()];
    System.arraycopy(buf, 0, newbuf, 0, size());
    return newbuf;
  }

  public String toString() {
    return new String(buf, 0, size());
  }
}