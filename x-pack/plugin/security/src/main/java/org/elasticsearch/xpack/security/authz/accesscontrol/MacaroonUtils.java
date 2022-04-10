/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authz.accesscontrol;

import com.github.nitram509.jmacaroons.Macaroon;
import com.github.nitram509.jmacaroons.MacaroonsBuilder;
import com.github.nitram509.jmacaroons.verifier.TimestampCaveatVerifier;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.InputStreamStreamInput;
import org.elasticsearch.common.io.stream.StreamInput;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.LocalDate;
import java.util.Base64;
import java.util.Calendar;
import java.util.Date;

import static org.elasticsearch.xpack.security.authc.TokenService.VERSION_MACAROON_ACCESS_TOKENS;

public final class MacaroonUtils {

    public static final String PIT_CAVEAT_KEY = "restrict access to PIT";
    public static final String READ_ONLY_CAVEAT_KEY = "restrict read-only";

    private static final SimpleDateFormat ISO_DateFormat_MILLIS_TIMEZONE = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSX");

    public static String attenuateTokenForReadOnly(String token) throws IOException {
        Date now = Calendar.getInstance().getTime();
        return attenuateTokenWithCaveats(token, READ_ONLY_CAVEAT_KEY);
    }

    public static String attenuateTokenForPITOnly(String token, String pitId) throws IOException {
        return attenuateTokenWithCaveats(token, READ_ONLY_CAVEAT_KEY, PIT_CAVEAT_KEY + " = " + pitId);
    }

    public static String attenuateTokenInDuration(String token, Duration validFor) throws IOException {
        if (validFor.isNegative()) {
            throw new IllegalArgumentException("Token validity must be positive");
        }
        String expiryDate = ISO_DateFormat_MILLIS_TIMEZONE.format(LocalDate.now().plus(validFor));
        return attenuateTokenWithCaveats(token, TimestampCaveatVerifier.CAVEAT_PREFIX + expiryDate);
    }

    public static String attenuateTokenWithCaveats(String token, String... caveats) throws IOException {
        final byte[] bytes = token.getBytes(StandardCharsets.UTF_8);
        try (StreamInput in = new InputStreamStreamInput(Base64.getDecoder().wrap(new ByteArrayInputStream(bytes)), bytes.length)) {
            final Version version = Version.readVersion(in);
            if (false == version.onOrAfter(VERSION_MACAROON_ACCESS_TOKENS)) {
                throw new IllegalArgumentException("Unsupported token for attenuation");
            }
            in.setVersion(version);
            final byte[] decodedSalt = in.readByteArray();
            final byte[] passphraseHash = in.readByteArray();
            final String serializedMacaroon = in.readString();
            final Macaroon originalMacaroon = MacaroonsBuilder.deserialize(serializedMacaroon);

            final MacaroonsBuilder attenuatedMacaroonsBuilder = MacaroonsBuilder.modify(originalMacaroon);
            for (String caveat : caveats) {
                attenuatedMacaroonsBuilder.add_first_party_caveat(caveat);
            }
            try (BytesStreamOutput out = new BytesStreamOutput()) {
                out.setVersion(version);
                Version.writeVersion(version, out);
                out.writeByteArray(decodedSalt);
                out.writeByteArray(passphraseHash);
                out.writeString(attenuatedMacaroonsBuilder.getMacaroon().serialize());
                return Base64.getEncoder().encodeToString(out.bytes().toBytesRef().bytes);
            }
        }
    }

}
