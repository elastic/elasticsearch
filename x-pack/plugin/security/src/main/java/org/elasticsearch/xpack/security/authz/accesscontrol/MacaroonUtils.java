/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authz.accesscontrol;

import com.github.nitram509.jmacaroons.Macaroon;
import com.github.nitram509.jmacaroons.MacaroonsBuilder;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.InputStreamStreamInput;
import org.elasticsearch.common.io.stream.StreamInput;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

import static org.elasticsearch.xpack.security.authc.TokenService.VERSION_MACAROON_ACCESS_TOKENS;

public final class MacaroonUtils {

    public static String attenuateTokenWithCaveat(String token, String caveat) throws IOException {
        final byte[] bytes = token.getBytes(StandardCharsets.UTF_8);
        try (StreamInput in = new InputStreamStreamInput(Base64.getDecoder().wrap(new ByteArrayInputStream(bytes)), bytes.length)) {
            final Version version = Version.readVersion(in);
            if (false == version.onOrAfter(VERSION_MACAROON_ACCESS_TOKENS)) {
                throw new IllegalArgumentException("Unsupported token attenuation");
            }
            in.setVersion(version);
            final byte[] decodedSalt = in.readByteArray();
            final byte[] passphraseHash = in.readByteArray();
            final String serializedMacaroon = in.readString();
            final Macaroon originalMacaroon = MacaroonsBuilder.deserialize(serializedMacaroon);

            final Macaroon attenuatedMacaroon = MacaroonsBuilder.modify(originalMacaroon).add_first_party_caveat(caveat).getMacaroon();
            try (BytesStreamOutput out = new BytesStreamOutput()) {
                out.setVersion(version);
                Version.writeVersion(version, out);
                out.writeByteArray(decodedSalt);
                out.writeByteArray(passphraseHash);
                out.writeString(attenuatedMacaroon.serialize());
                return Base64.getEncoder().encodeToString(out.bytes().toBytesRef().bytes);
            }
        }
    }

}
