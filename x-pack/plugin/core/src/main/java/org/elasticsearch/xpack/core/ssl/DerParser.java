/* @notice
  Copyright (c) 1998-2010 AOL Inc.

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.

 */

package org.elasticsearch.xpack.core.ssl;


import org.elasticsearch.common.hash.MessageDigests;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigInteger;

/**
 * A bare-minimum ASN.1 DER decoder, just having enough functions to
 * decode PKCS#1 private keys in order to remain JCE/JVM agnostic.
 * <p>
 * Based on https://github.com/groovenauts/jmeter_oauth_plugin/blob/master/jmeter/src/
 * main/java/org/apache/jmeter/protocol/oauth/sampler/PrivateKeyReader.java
 */
class DerParser {
    // Constructed Flag
    private static final int CONSTRUCTED = 0x20;

    // Tag and data types
    private static final int INTEGER = 0x02;
    private static final int OCTET_STRING = 0x04;
    private static final int OBJECT_OID = 0x06;
    private static final int NUMERIC_STRING = 0x12;
    private static final int PRINTABLE_STRING = 0x13;
    private static final int VIDEOTEX_STRING = 0x15;
    private static final int IA5_STRING = 0x16;
    private static final int GRAPHIC_STRING = 0x19;
    private static final int ISO646_STRING = 0x1A;
    private static final int GENERAL_STRING = 0x1B;

    private static final int UTF8_STRING = 0x0C;
    private static final int UNIVERSAL_STRING = 0x1C;
    private static final int BMP_STRING = 0x1E;


    private InputStream derInputStream;
    private int maxAsnObjectLength;

    DerParser(byte[] bytes) {
        this.derInputStream = new ByteArrayInputStream(bytes);
        this.maxAsnObjectLength = bytes.length;
    }

    Asn1Object readAsn1Object() throws IOException {
        int tag = derInputStream.read();
        if (tag == -1) {
            throw new IOException("Invalid DER: stream too short, missing tag");
        }
        int length = getLength();
        // getLength() can return any 32 bit integer, so ensure that a corrupted encoding won't
        // force us into allocating a very large array
        if (length > maxAsnObjectLength) {
            throw new IOException("Invalid DER: size of ASN.1 object to be parsed appears to be larger than the size of the key file " +
                "itself.");
        }
        byte[] value = new byte[length];
        int n = derInputStream.read(value);
        if (n < length) {
            throw new IOException("Invalid DER: stream too short, missing value. " +
                    "Could only read " + n + " out of " + length + " bytes");
        }
        return new Asn1Object(tag, length, value);

    }

    /**
     * Decode the length of the field. Can only support length
     * encoding up to 4 octets.
     * <p>
     *          In BER/DER encoding, length can be encoded in 2 forms:
     * </p>
     * <ul>
     * <li>Short form. One octet. Bit 8 has value "0" and bits 7-1
     * give the length.
     * </li>
     * <li>Long form. Two to 127 octets (only 4 is supported here).
     * Bit 8 of first octet has value "1" and bits 7-1 give the
     * number of additional length octets. Second and following
     * octets give the length, base 256, most significant digit first.
     * </li>
     * </ul>
     *
     * @return The length as integer
     */
    private int getLength() throws IOException {

        int i = derInputStream.read();
        if (i == -1)
            throw new IOException("Invalid DER: length missing");

        // A single byte short length
        if ((i & ~0x7F) == 0)
            return i;

        int num = i & 0x7F;

        // We can't handle length longer than 4 bytes
        if (i >= 0xFF || num > 4)
            throw new IOException("Invalid DER: length field too big ("
                    + i + ")"); //$NON-NLS-1$

        byte[] bytes = new byte[num];
        int n = derInputStream.read(bytes);
        if (n < num)
            throw new IOException("Invalid DER: length too short");

        return new BigInteger(1, bytes).intValue();
    }


    /**
     * An ASN.1 TLV. The object is not parsed. It can
     * only handle integers.
     *
     * @author zhang
     */
    static class Asn1Object {

        protected final int type;
        protected final int length;
        protected final byte[] value;
        protected final int tag;

        /**
         * Construct a ASN.1 TLV. The TLV could be either a
         * constructed or primitive entity.
         * <p>
         *     The first byte in DER encoding is made of following fields:
         * </p>
         * <pre>
         * -------------------------------------------------
         * |Bit 8|Bit 7|Bit 6|Bit 5|Bit 4|Bit 3|Bit 2|Bit 1|
         * -------------------------------------------------
         * |  Class    | CF  |     +      Type             |
         * -------------------------------------------------
         * </pre>
         * <ul>
         * <li>Class: Universal, Application, Context or Private
         * <li>CF: Constructed flag. If 1, the field is constructed.
         * <li>Type: This is actually called tag in ASN.1. It
         * indicates data type (Integer, String) or a construct
         * (sequence, choice, set).
         * </ul>
         *
         * @param tag    Tag or Identifier
         * @param length Length of the field
         * @param value  Encoded octet string for the field.
         */
        Asn1Object(int tag, int length, byte[] value) {
            this.tag = tag;
            this.type = tag & 0x1F;
            this.length = length;
            this.value = value;
        }

        public int getType() {
            return type;
        }

        public int getLength() {
            return length;
        }

        public byte[] getValue() {
            return value;
        }

        public boolean isConstructed() {
            return (tag & DerParser.CONSTRUCTED) == DerParser.CONSTRUCTED;
        }

        /**
         * For constructed field, return a parser for its content.
         *
         * @return A parser for the construct.
         */
        public DerParser getParser() throws IOException {
            if (isConstructed() == false) {
                throw new IOException("Invalid DER: can't parse primitive entity"); //$NON-NLS-1$
            }

            return new DerParser(value);
        }

        /**
         * Get the value as integer
         *
         * @return BigInteger
         */
        public BigInteger getInteger() throws IOException {
            if (type != DerParser.INTEGER)
                throw new IOException("Invalid DER: object is not integer"); //$NON-NLS-1$

            return new BigInteger(value);
        }

        public String getString() throws IOException {

            String encoding;

            switch (type) {
                case DerParser.OCTET_STRING:
                    // octet string is basically a byte array
                    return MessageDigests.toHexString(value);
                case DerParser.NUMERIC_STRING:
                case DerParser.PRINTABLE_STRING:
                case DerParser.VIDEOTEX_STRING:
                case DerParser.IA5_STRING:
                case DerParser.GRAPHIC_STRING:
                case DerParser.ISO646_STRING:
                case DerParser.GENERAL_STRING:
                    encoding = "ISO-8859-1"; //$NON-NLS-1$
                    break;

                case DerParser.BMP_STRING:
                    encoding = "UTF-16BE"; //$NON-NLS-1$
                    break;

                case DerParser.UTF8_STRING:
                    encoding = "UTF-8"; //$NON-NLS-1$
                    break;

                case DerParser.UNIVERSAL_STRING:
                    throw new IOException("Invalid DER: can't handle UCS-4 string"); //$NON-NLS-1$

                default:
                    throw new IOException("Invalid DER: object is not a string"); //$NON-NLS-1$
            }

            return new String(value, encoding);
        }

        public String getOid() throws IOException {

            if (type != DerParser.OBJECT_OID) {
                throw new IOException("Ivalid DER: object is not object OID");
            }
            StringBuilder sb = new StringBuilder(64);
            switch (value[0] / 40) {
                case 0:
                    sb.append('0');
                    break;
                case 1:
                    sb.append('1');
                    value[0] -= 40;
                    break;
                default:
                    sb.append('2');
                    value[0] -= 80;
                    break;
            }
            int oidPart = 0;
            for (int i = 0; i < length; i++) {
                oidPart = (oidPart << 7) + (value[i] & 0x7F);
                if ((value[i] & 0x80) == 0) {
                    sb.append('.');
                    sb.append(oidPart);
                    oidPart = 0;
                }
            }

            return sb.toString();
        }
    }
}
