/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc.pki;

import org.elasticsearch.common.ssl.DerParser;

import java.io.IOException;

/**
 * Utility class to extract RDN field values from X500 principal DER encoding.
 */
public class RdnFieldExtractor {

    public static String extract(byte[] encoded, String oid) {
        try {
            return doExtract(encoded, oid);
        } catch (IOException | IllegalStateException e) {
            return null; // invalid encoding
        }
    }

    private static String doExtract(byte[] encoded, String oid) throws IOException {
        DerParser parser = new DerParser(encoded);

        DerParser.Asn1Object dnSequence = parser.readAsn1Object(DerParser.Type.SEQUENCE);
        DerParser sequenceParser = dnSequence.getParser();

        String value = null;

        while (true) {
            try {
                DerParser.Asn1Object rdnSet = sequenceParser.readAsn1Object(DerParser.Type.SET); // throws IOException on EOF
                DerParser setParser = rdnSet.getParser();

                while (true) {
                    try {
                        DerParser.Asn1Object attrSeq = setParser.readAsn1Object(DerParser.Type.SEQUENCE);  // throws IOException on EOF
                        DerParser attrParser = attrSeq.getParser();

                        String attrOid = attrParser.readAsn1Object().getOid();
                        DerParser.Asn1Object attrValue = attrParser.readAsn1Object();
                        if (oid.equals(attrOid)) {
                            value = attrValue.getString(); // retain last (most-significant) occurrence
                        }
                    } catch (IOException e) {
                        break; // RDN SET EOF
                    }
                }
            } catch (IOException e) {
                break; // DN SEQUENCE EOF
            }
        }

        return value;
    }

}
