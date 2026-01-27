/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License v 1".
 */
package org.elasticsearch.plugin.security.cloudiam;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.security.authc.RealmConfig;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

public class AliyunStsClient implements IamClient {
    private static final String PARAM_ACTION = "Action";
    private static final String PARAM_VERSION = "Version";
    private static final String PARAM_FORMAT = "Format";
    private static final String PARAM_ACCESS_KEY_ID = "AccessKeyId";
    private static final String PARAM_SIGNATURE = "Signature";
    private static final String PARAM_SIGNATURE_METHOD = "SignatureMethod";
    private static final String PARAM_SIGNATURE_VERSION = "SignatureVersion";
    private static final String PARAM_SIGNATURE_NONCE = "SignatureNonce";
    private static final String PARAM_TIMESTAMP = "Timestamp";
    private static final String PARAM_SECURITY_TOKEN = "SecurityToken";

    private static final String ACTION_GET_CALLER_IDENTITY = "GetCallerIdentity";
    private static final String STS_VERSION = "2015-04-01";
    private static final Set<String> ALLOWED_SIGNATURE_METHODS = Set.of("HMAC-SHA1", "HMAC-SHA256");
    private static final Set<String> ALLOWED_PARAMS = Set.of(
        PARAM_ACTION,
        PARAM_VERSION,
        PARAM_FORMAT,
        PARAM_ACCESS_KEY_ID,
        PARAM_SIGNATURE,
        PARAM_SIGNATURE_METHOD,
        PARAM_SIGNATURE_VERSION,
        PARAM_SIGNATURE_NONCE,
        PARAM_TIMESTAMP,
        PARAM_SECURITY_TOKEN
    );

    private final String endpoint;
    private final TimeValue readTimeout;
    private final HttpClient httpClient;

    public AliyunStsClient(RealmConfig config) {
        String endpointSetting = config.getSetting(CloudIamRealmSettings.IAM_ENDPOINT, () -> "");
        String region = config.getSetting(CloudIamRealmSettings.IAM_REGION, () -> "");
        this.endpoint = resolveEndpoint(endpointSetting, region);
        TimeValue connectTimeout = config.getSetting(CloudIamRealmSettings.IAM_CONNECT_TIMEOUT);
        this.readTimeout = config.getSetting(CloudIamRealmSettings.IAM_READ_TIMEOUT);
        this.httpClient = HttpClient.newBuilder()
            .connectTimeout(Duration.ofMillis(connectTimeout.getMillis()))
            .build();
    }

    @Override
    public void verify(CloudIamToken token, ActionListener<IamPrincipal> listener) {
        try {
            Map<String, String> params = validateAndFilter(token.signedParams(), token.accessKeyId());
            String url = endpoint + "/?" + toQueryString(params);
            HttpRequest request = HttpRequest.newBuilder(URI.create(url))
                .timeout(Duration.ofMillis(readTimeout.getMillis()))
                .GET()
                .build();
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
            if (response.statusCode() != 200) {
                listener.onFailure(new IllegalStateException("iam verify failed with status " + response.statusCode()));
                return;
            }
            listener.onResponse(parsePrincipal(response.body()));
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    private static String resolveEndpoint(String endpoint, String region) {
        if (Strings.hasText(endpoint)) {
            return normalizeEndpoint(endpoint);
        }
        if (Strings.hasText(region)) {
            return "https://sts." + region + ".aliyuncs.com";
        }
        return "https://sts.aliyuncs.com";
    }

    private static String normalizeEndpoint(String endpoint) {
        String trimmed = endpoint.trim();
        if (trimmed.isEmpty()) {
            return trimmed;
        }
        String normalized = trimmed.startsWith("http://") || trimmed.startsWith("https://") ? trimmed : "https://" + trimmed;
        if (normalized.endsWith("/")) {
            return normalized.substring(0, normalized.length() - 1);
        }
        return normalized;
    }

    private static Map<String, String> validateAndFilter(Map<String, String> params, String expectedAccessKeyId) {
        if (params == null || params.isEmpty()) {
            throw new IllegalArgumentException("missing signed parameters");
        }
        for (String key : params.keySet()) {
            if (ALLOWED_PARAMS.contains(key) == false) {
                throw new IllegalArgumentException("unsupported signed parameter: " + key);
            }
        }
        require(params, PARAM_ACTION);
        require(params, PARAM_VERSION);
        require(params, PARAM_ACCESS_KEY_ID);
        require(params, PARAM_SIGNATURE);
        require(params, PARAM_SIGNATURE_METHOD);
        require(params, PARAM_SIGNATURE_VERSION);
        require(params, PARAM_SIGNATURE_NONCE);
        require(params, PARAM_TIMESTAMP);
        if (ACTION_GET_CALLER_IDENTITY.equals(params.get(PARAM_ACTION)) == false) {
            throw new IllegalArgumentException("unsupported action");
        }
        if (STS_VERSION.equals(params.get(PARAM_VERSION)) == false) {
            throw new IllegalArgumentException("unsupported version");
        }
        if (ALLOWED_SIGNATURE_METHODS.contains(params.get(PARAM_SIGNATURE_METHOD)) == false) {
            throw new IllegalArgumentException("unsupported signature method");
        }
        if ("1.0".equals(params.get(PARAM_SIGNATURE_VERSION)) == false) {
            throw new IllegalArgumentException("unsupported signature version");
        }
        String format = params.get(PARAM_FORMAT);
        if (Strings.hasText(format) && "JSON".equals(format) == false) {
            throw new IllegalArgumentException("unsupported format");
        }
        if (expectedAccessKeyId != null && expectedAccessKeyId.equals(params.get(PARAM_ACCESS_KEY_ID)) == false) {
            throw new IllegalArgumentException("access key mismatch");
        }
        return new TreeMap<>(params);
    }

    private static void require(Map<String, String> params, String key) {
        if (Strings.hasText(params.get(key)) == false) {
            throw new IllegalArgumentException("missing required parameter: " + key);
        }
    }

    private static IamPrincipal parsePrincipal(String body) throws IOException {
        Tuple<XContentType, Map<String, Object>> parsed = XContentHelper.convertToMap(new BytesArray(body), false, XContentType.JSON);
        Map<String, Object> source = parsed.v2();
        Object nested = source.get("GetCallerIdentityResponse");
        if (nested instanceof Map<?, ?> nestedMap) {
            Map<String, Object> extracted = new TreeMap<>();
            for (Map.Entry<?, ?> entry : nestedMap.entrySet()) {
                if (entry.getKey() == null) {
                    continue;
                }
                extracted.put(entry.getKey().toString(), entry.getValue());
            }
            source = extracted;
        }
        String arn = stringValue(source.get("Arn"));
        String accountId = stringValue(source.get("AccountId"));
        String userId = stringValue(source.get("UserId"));
        if (Strings.hasText(arn) == false) {
            throw new IllegalStateException("missing arn in iam response");
        }
        if (Strings.hasText(accountId) == false) {
            accountId = parseAccountId(arn);
        }
        if (Strings.hasText(accountId) == false) {
            throw new IllegalStateException("missing account id in iam response");
        }
        IamPrincipal.PrincipalType type = IamPrincipal.principalTypeFromArn(arn);
        return new IamPrincipal(arn, accountId, userId, type);
    }

    private static String parseAccountId(String arn) {
        // Aliyun ARN format: acs:service:region:account-id:resource
        // AWS ARN format: arn:partition:service:region:account-id:resource
        // Extract account-id which is the 4th colon-separated part (0-indexed: parts[3])
        String[] parts = arn.split(":", 5);
        if (parts.length < 5) {
            return null;
        }
        return parts[3];
    }

    private static String stringValue(Object value) {
        return value == null ? null : value.toString();
    }

    private static String toQueryString(Map<String, String> params) {
        StringBuilder builder = new StringBuilder();
        boolean first = true;
        for (Map.Entry<String, String> entry : params.entrySet()) {
            if (first == false) {
                builder.append('&');
            }
            first = false;
            builder.append(percentEncode(entry.getKey()))
                .append('=')
                .append(percentEncode(entry.getValue()));
        }
        return builder.toString();
    }

    private static String percentEncode(String value) {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        StringBuilder builder = new StringBuilder(bytes.length);
        for (byte b : bytes) {
            int unsigned = b & 0xff;
            char c = (char) unsigned;
            if ((c >= 'A' && c <= 'Z')
                || (c >= 'a' && c <= 'z')
                || (c >= '0' && c <= '9')
                || c == '-' || c == '_' || c == '.' || c == '~') {
                builder.append(c);
            } else {
                builder.append('%');
                builder.append(String.format(Locale.ROOT, "%02X", unsigned));
            }
        }
        return builder.toString();
    }
}
