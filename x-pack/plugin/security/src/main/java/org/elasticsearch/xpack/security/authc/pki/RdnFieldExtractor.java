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

    // ASN.1 type is the lower 5 bits of the identifier octet
    // See: ITU-T X.690
    private static final int ASN1_TYPE_SEQUENCE = 0x10;
    private static final int ASN1_TYPE_SET = 0x11;

    public static String extract(byte[] encoded, String oid) {
        try {
            return doExtract(encoded, oid);
        } catch (IOException | IllegalStateException e) {
            return null; // EOF or invalid encoding
        }
    }

    private static String doExtract(byte[] encoded, String oid) throws IOException {
        DerParser parser = new DerParser(encoded);

        DerParser.Asn1Object dnSequence = parser.readAsn1Object(ASN1_TYPE_SEQUENCE);
        DerParser sequenceParser = dnSequence.getParser();

        while (true) {
            DerParser.Asn1Object rdnSet = sequenceParser.readAsn1Object(ASN1_TYPE_SET); // allow EOF to propagate
            DerParser setParser = rdnSet.getParser();

            while (true) {
                try {
                    DerParser.Asn1Object attrSeq = setParser.readAsn1Object(ASN1_TYPE_SEQUENCE);
                    DerParser attrParser = attrSeq.getParser();

                    String attrOid = attrParser.readAsn1Object().getOid();
                    DerParser.Asn1Object attrValue = attrParser.readAsn1Object();
                    if (oid.equals(attrOid)) {
                        return attrValue.getString();
                    }
                } catch (IOException e) {
                    break; // EOF
                }
            }
        }
    }

}
