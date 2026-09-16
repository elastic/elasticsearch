/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.inference;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Strings;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.AbstractBWCSerializationTestCase;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.elasticsearch.inference.DataFormat.URL_INPUT_FORMAT_FEATURE_FLAG;
import static org.elasticsearch.inference.InferenceString.EMBEDDING_AUDIO_VIDEO_PDF_INPUT_SUPPORT_ADDED;
import static org.elasticsearch.inference.InferenceString.FORMAT_FIELD;
import static org.elasticsearch.inference.InferenceString.TYPE_FIELD;
import static org.elasticsearch.inference.InferenceString.URL_INPUT_FORMAT_SUPPORT_ADDED;
import static org.elasticsearch.inference.InferenceString.VALUE_FIELD;
import static org.elasticsearch.inference.InferenceString.fromStringList;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class InferenceStringTests extends AbstractBWCSerializationTestCase<InferenceString> {
    public static final String TEST_DATA_URI = "data:mime/type;base64,abcd";

    public void testConstructorWithNoFormat_usesDefault() {
        assertThat(InferenceString.ofText("value").dataFormat(), is(DataFormat.TEXT));
        assertThat(new InferenceString(DataType.IMAGE, TEST_DATA_URI).dataFormat(), is(DataFormat.BASE64));
        assertThat(new InferenceString(DataType.AUDIO, TEST_DATA_URI).dataFormat(), is(DataFormat.BASE64));
        assertThat(new InferenceString(DataType.VIDEO, TEST_DATA_URI).dataFormat(), is(DataFormat.BASE64));
        assertThat(new InferenceString(DataType.PDF, TEST_DATA_URI).dataFormat(), is(DataFormat.BASE64));
    }

    public void testSupportedFormatsForType() {
        assertThat(DataType.TEXT.getSupportedFormats(), is(EnumSet.of(DataFormat.TEXT)));
        assertThat(DataType.IMAGE.getSupportedFormats(), is(EnumSet.of(DataFormat.BASE64, DataFormat.URL)));
        assertThat(DataType.AUDIO.getSupportedFormats(), is(EnumSet.of(DataFormat.BASE64, DataFormat.URL)));
        assertThat(DataType.VIDEO.getSupportedFormats(), is(EnumSet.of(DataFormat.BASE64, DataFormat.URL)));
        assertThat(DataType.PDF.getSupportedFormats(), is(EnumSet.of(DataFormat.BASE64, DataFormat.URL)));
    }

    public void testConstructorWithInvalidDataURI_throws() {
        var invalidDataURIs = List.of(
            "notADataURI",
            "image/jpeg;base64,abcd", // missing "data:"
            "data:image/jpeg;base64abcd", // missing final ","
            "data:;base64,abcd", // missing MIME type
            "data:image/jpeg;abcd", // missing "base64,"
            "Xdata:image/jpeg;base64,abcd", // extra character at start
            "data;image/jpeg;base64,abcd", // doesn't start with "data:"
            "data:image/jpeg;base63,abcd", // doesn't contain "base64,"
            "data:invalid;base64,abcd" // invalid MIME type format
        );
        invalidDataURIs.forEach(value -> {
            var exception = assertThrows(
                IllegalArgumentException.class,
                () -> new InferenceString(DataType.IMAGE, DataFormat.BASE64, value)
            );
            assertThat(
                exception.getMessage(),
                is("base64 inputs must be specified as data URIs with the format [data:{MIME-type};base64,...]")
            );
        });
    }

    public void testConstructorWithValidDataURIFormat() {
        var value = Strings.format(
            "data:%s/%s;base64,%s",
            randomAlphanumericOfLength(10),
            randomAlphanumericOfLength(10),
            randomAlphanumericOfLength(10)
        );
        new InferenceString(DataType.IMAGE, DataFormat.BASE64, value);
    }

    /** RFC 2397 parameters and MIME types containing {@code +} must still be accepted. */
    public void testConstructorWithValidDataURIFormat_withMediaTypeParameters() {
        new InferenceString(DataType.IMAGE, DataFormat.BASE64, "data:image/png;charset=utf-8;base64,abcd");
        new InferenceString(DataType.IMAGE, DataFormat.BASE64, "data:image/png;p1=v1;p2=v2;base64,abcd");
        new InferenceString(DataType.IMAGE, DataFormat.BASE64, "data:image/svg+xml;base64,abcd");
    }

    public void testTryParseDataUri_extractsMediaTypeAndPayload() {
        assertThat(InferenceString.tryParseDataUri("data:image/png;base64,abcd"), is(new InferenceString.DataUri("image/png", "abcd")));
        // RFC 2397 parameters are preserved as declared; interpreting them is up to the caller.
        assertThat(
            InferenceString.tryParseDataUri("data:text/plain;charset=utf-8;base64,abcd"),
            is(new InferenceString.DataUri("text/plain;charset=utf-8", "abcd"))
        );
    }

    public void testTryParseDataUri_returnsNullForInvalidValues() {
        var invalidValues = List.of(
            "",
            "notADataURI",
            "abcd", // bare base64 without a data URI prefix
            "https://example.com/image.png", // plain URL
            "data:image/jpeg;base64abcd", // missing final ","
            "data:;base64,abcd", // missing MIME type
            "data:image/" + "a".repeat(InferenceString.MAX_DATA_URI_PREFIX_LENGTH) + ";base64,abcd" // oversized prefix
        );
        invalidValues.forEach(value -> assertThat(value, InferenceString.tryParseDataUri(value), nullValue()));
    }

    /** URI prefixes exceeding {@link InferenceString#MAX_DATA_URI_PREFIX_LENGTH} are rejected before the regex runs. */
    public void testConstructorWithOversizedDataURIPrefix_throws() {
        String oversizedPrefixValue = "data:image/" + "a".repeat(InferenceString.MAX_DATA_URI_PREFIX_LENGTH) + ";base64,abcd";

        var exception = assertThrows(
            IllegalArgumentException.class,
            () -> new InferenceString(DataType.IMAGE, DataFormat.BASE64, oversizedPrefixValue)
        );
        assertThat(
            exception.getMessage(),
            is("base64 inputs must be specified as data URIs with the format [data:{MIME-type};base64,...]")
        );
    }

    /** Adversarial input that would backtrack under the old {@code .*&#47;.*} regex must fail fast. */
    public void testConstructorWithPathologicalDataURI_throwsAndCompletesQuickly() {
        String pathological = "data:a" + "/a;".repeat(100) + ",";

        var exception = assertThrows(
            IllegalArgumentException.class,
            () -> new InferenceString(DataType.IMAGE, DataFormat.BASE64, pathological)
        );
        assertThat(
            exception.getMessage(),
            is("base64 inputs must be specified as data URIs with the format [data:{MIME-type};base64,...]")
        );
    }

    public void testParserWithText() throws IOException {
        var requestJson = """
            {
                "type": "text",
                "format": "text",
                "value": "some text input"
            }
            """;
        try (var parser = createParser(JsonXContent.jsonXContent, requestJson)) {
            var request = InferenceString.PARSER.apply(parser, null);
            assertThat(request.dataType(), is(DataType.TEXT));
            assertThat(request.dataFormat(), is(DataFormat.TEXT));
            assertThat(request.value(), is("some text input"));
        }
    }

    public void testParserWithBase64Image() throws IOException {
        testParserWithBase64Format(DataType.IMAGE);
    }

    public void testParserWithBase64Audio() throws IOException {
        testParserWithBase64Format(DataType.AUDIO);
    }

    public void testParserWithBase64Video() throws IOException {
        testParserWithBase64Format(DataType.VIDEO);
    }

    private void testParserWithBase64Format(DataType type) throws IOException {
        var requestJson = Strings.format("""
            {
                "type": "%s",
                "format": "base64",
                "value": "%s"
            }
            """, type.toString(), TEST_DATA_URI);
        try (var parser = createParser(JsonXContent.jsonXContent, requestJson)) {
            var request = InferenceString.PARSER.apply(parser, null);
            assertThat(request.dataType(), is(type));
            assertThat(request.dataFormat(), is(DataFormat.BASE64));
            assertThat(request.value(), is(TEST_DATA_URI));
        }
    }

    public void testParserWithDefaultTextFormat() throws IOException {
        testParserWithDefaultFormat(DataType.TEXT, DataFormat.TEXT, "some text input");
    }

    public void testParserWithDefaultImageFormat() throws IOException {
        testParserWithDefaultFormat(DataType.IMAGE, DataFormat.BASE64, TEST_DATA_URI);
    }

    public void testParserWithDefaultAudioFormat() throws IOException {
        testParserWithDefaultFormat(DataType.AUDIO, DataFormat.BASE64, TEST_DATA_URI);
    }

    public void testParserWithDefaultVideoFormat() throws IOException {
        testParserWithDefaultFormat(DataType.VIDEO, DataFormat.BASE64, TEST_DATA_URI);
    }

    public void testParserWithDefaultPdfFormat() throws IOException {
        testParserWithDefaultFormat(DataType.PDF, DataFormat.BASE64, TEST_DATA_URI);
    }

    private void testParserWithDefaultFormat(DataType type, DataFormat expectedFormat, String value) throws IOException {
        var requestJson = Strings.format("""
            {
                "type": "%s",
                "value": "%s"
            }
            """, type.toString(), value);
        try (var parser = createParser(JsonXContent.jsonXContent, requestJson)) {
            var request = InferenceString.PARSER.apply(parser, null);
            assertThat(request.dataType(), is(type));
            assertThat(request.dataFormat(), is(expectedFormat));
            assertThat(request.value(), is(value));
        }
    }

    public void testParserWithNoType_throwsException() throws IOException {
        var requestJson = Strings.format("""
            {
                "value": "%s"
            }
            """, TEST_DATA_URI);
        try (var parser = createParser(JsonXContent.jsonXContent, requestJson)) {
            IllegalArgumentException exception = expectThrows(
                IllegalArgumentException.class,
                () -> InferenceString.PARSER.apply(parser, null)
            );
            assertThat(exception.getMessage(), is("Required [type]"));
        }
    }

    public void testParserWithNoValue_throwsException() throws IOException {
        var requestJson = """
            {
                "type": "text"
            }
            """;
        try (var parser = createParser(JsonXContent.jsonXContent, requestJson)) {
            IllegalArgumentException exception = expectThrows(
                IllegalArgumentException.class,
                () -> InferenceString.PARSER.apply(parser, null)
            );
            assertThat(exception.getMessage(), is("Required [value]"));
        }
    }

    public void testParserWithUnknownField_throwsException() throws IOException {
        var requestJson = Strings.format("""
            {
                "type": "image",
                "format": "base64",
                "value": "%s",
                "extra": "should throw"
            }
            """, TEST_DATA_URI);
        try (var parser = createParser(JsonXContent.jsonXContent, requestJson)) {
            IllegalArgumentException exception = expectThrows(
                IllegalArgumentException.class,
                () -> InferenceString.PARSER.apply(parser, null)
            );
            assertThat(exception.getMessage(), containsString("unknown field [extra]"));
        }
    }

    public void testParserWithUnknownType_throwsException() throws IOException {
        var invalidType = "not a real type";
        var requestJson = Strings.format("""
            {
                "type": "%s",
                "format": "text",
                "value": "some image data"
            }
            """, invalidType);
        try (var parser = createParser(JsonXContent.jsonXContent, requestJson)) {
            IllegalArgumentException exception = expectThrows(
                IllegalArgumentException.class,
                () -> InferenceString.PARSER.apply(parser, null)
            );
            assertThat(exception.getMessage(), containsString("[InferenceString] failed to parse field [type]"));
            assertThat(
                exception.getCause().getMessage(),
                is(Strings.format("Unrecognized type [%s], must be one of [text, image, audio, video, pdf]", invalidType))
            );
        }
    }

    public void testParserWithUnknownFormat_throwsException() throws IOException {
        var invalidFormat = "not a real format";
        var requestJson = Strings.format("""
            {
                "type": "text",
                "format": "%s"
                "value": "%s"
            }
            """, invalidFormat, TEST_DATA_URI);
        try (var parser = createParser(JsonXContent.jsonXContent, requestJson)) {
            IllegalArgumentException exception = expectThrows(
                IllegalArgumentException.class,
                () -> InferenceString.PARSER.apply(parser, null)
            );
            assertThat(exception.getMessage(), containsString("[InferenceString] failed to parse field [format]"));
            var expectedFormats = URL_INPUT_FORMAT_FEATURE_FLAG.isEnabled() ? "[text, base64, url]" : "[text, base64]";
            assertThat(
                exception.getCause().getMessage(),
                is(Strings.format("Unrecognized format [%s], must be one of %s", invalidFormat, expectedFormats))
            );
        }
    }

    public void testParserWithInvalidTypeAndFormatCombination_throwsException() throws IOException {
        var type = randomFrom(DataType.values());
        var unsupportedDataFormats = EnumSet.allOf(DataFormat.class);
        unsupportedDataFormats.removeAll(type.getSupportedFormats());
        var invalidFormat = randomFrom(unsupportedDataFormats);
        var requestJson = Strings.format("""
            {
                "type": "%s",
                "format": "%s",
                "value": "some data"
            }
            """, type, invalidFormat);
        try (var parser = createParser(JsonXContent.jsonXContent, requestJson)) {
            IllegalArgumentException exception = expectThrows(
                IllegalArgumentException.class,
                () -> InferenceString.PARSER.apply(parser, null)
            );
            assertThat(exception.getMessage(), containsString("[InferenceString] failed to parse field [value]"));
            Throwable cause = exception.getCause();
            assertThat(cause.getMessage(), is("Failed to build [InferenceString] after last required field arrived"));
            var displayedSupportedFormats = type.getSupportedFormats()
                .stream()
                .filter(f -> f != DataFormat.URL || URL_INPUT_FORMAT_FEATURE_FLAG.isEnabled())
                .toList();
            assertThat(
                cause.getCause().getMessage(),
                is(
                    Strings.format(
                        "Data type [%s] does not support data format [%s], supported formats are %s",
                        type,
                        invalidFormat,
                        displayedSupportedFormats
                    )
                )
            );
        }
    }

    public void testParserWithInvalidDataURI_throwsException() throws IOException {
        var base64Types = Arrays.stream(DataType.values())
            .filter(t -> t.getSupportedFormats().contains(DataFormat.BASE64))
            .toArray(DataType[]::new);
        var type = randomFrom(base64Types);
        var requestJson = Strings.format("""
            {
                "type": "%s",
                "format": "base64",
                "value": "not_a_data_uri"
            }
            """, type.toString());
        try (var parser = createParser(JsonXContent.jsonXContent, requestJson)) {
            var exception = expectThrows(IllegalArgumentException.class, () -> InferenceString.PARSER.apply(parser, null));
            assertThat(exception.getMessage(), containsString("[InferenceString] failed to parse field [value]"));
            Throwable cause = exception.getCause();
            assertThat(cause.getMessage(), containsString("Failed to build [InferenceString] after last required field arrived"));
            assertThat(
                cause.getCause().getMessage(),
                containsString("base64 inputs must be specified as data URIs with the format [data:{MIME-type};base64,...]")
            );
        }
    }

    public void testFromStringList_CreatesExpectedList() {
        var strings = randomList(1, 5, () -> randomAlphanumericOfLength(8));
        var inferenceStrings = fromStringList(strings);

        assertThat(inferenceStrings, hasSize(strings.size()));
        for (int i = 0; i < strings.size(); ++i) {
            var inferenceString = inferenceStrings.get(i);
            assertThat(inferenceString.dataType(), is(DataType.TEXT));
            assertThat(inferenceString.dataFormat(), is(DataFormat.TEXT));
            assertThat(inferenceString.value(), is(strings.get(i)));
        }
    }

    public void testToStringList_withAllTextInferenceStrings() {
        var rawStrings = List.of("one", "two", "three", "four");
        var inferenceStrings = rawStrings.stream().map(InferenceString::ofText).toList();
        assertThat(InferenceString.toStringList(inferenceStrings), is(rawStrings));
    }

    public void testToStringList_throwsAssertionError_whenAnyInferenceStringIsNotText() {
        var rawStrings = List.of("one", "two", "three", "four");
        var inferenceStrings = rawStrings.stream().map(InferenceString::ofText).collect(Collectors.toList());
        // Add a non-text InferenceString randomly in the list
        inferenceStrings.add(randomInt(inferenceStrings.size()), new InferenceString(DataType.IMAGE, TEST_DATA_URI));
        AssertionError assertionError = expectThrows(AssertionError.class, () -> InferenceString.toStringList(inferenceStrings));
        assertThat(assertionError.getMessage(), is("Non-text input returned from InferenceString.textValue"));
    }

    /**
     * Versions before {@link InferenceString#EMBEDDING_AUDIO_VIDEO_PDF_INPUT_SUPPORT_ADDED} throw an exception when serializing audio,
     * video or pdf content, and versions before {@link InferenceString#URL_INPUT_FORMAT_SUPPORT_ADDED} throw an exception when
     * serializing URL-format inputs, so we filter those out of the bwc versions to avoid test failures.
     * The logic is tested directly by {@link #testAudioVideoPdfAreNotBackwardsCompatible} and
     * {@link #testUrlFormatIsNotBackwardsCompatible}
     */
    @Override
    protected Collection<TransportVersion> bwcVersions() {
        return super.bwcVersions().stream()
            .filter(version -> version.supports(EMBEDDING_AUDIO_VIDEO_PDF_INPUT_SUPPORT_ADDED))
            .filter(version -> version.supports(URL_INPUT_FORMAT_SUPPORT_ADDED))
            .toList();
    }

    /**
     * Verifies that audio, video and pdf inputs cannot be sent to nodes that do not support
     * {@link InferenceString#EMBEDDING_AUDIO_VIDEO_PDF_INPUT_SUPPORT_ADDED}.
     * <p>
     * We use specific BASE64-format instances rather than random ones to avoid interference from the later
     * {@link InferenceString#URL_INPUT_FORMAT_SUPPORT_ADDED} gate: random generation could produce URL-format instances,
     * which would fail with the URL-format error rather than the audio/video/pdf error and break the assertion.
     */
    public void testAudioVideoPdfAreNotBackwardsCompatible() throws IOException {
        var preAvpVersions = super.bwcVersions().stream()
            .filter(v -> v.supports(EMBEDDING_AUDIO_VIDEO_PDF_INPUT_SUPPORT_ADDED) == false)
            .toList();
        var base64Instances = List.of(
            new InferenceString(DataType.AUDIO, DataFormat.BASE64, TEST_DATA_URI),
            new InferenceString(DataType.VIDEO, DataFormat.BASE64, TEST_DATA_URI),
            new InferenceString(DataType.PDF, DataFormat.BASE64, TEST_DATA_URI)
        );
        for (var instance : base64Instances) {
            for (var version : preAvpVersions) {
                var ex = assertThrows(
                    ElasticsearchStatusException.class,
                    () -> copyWriteable(instance, getNamedWriteableRegistry(), instanceReader(), version)
                );
                assertThat(ex.status(), is(RestStatus.BAD_REQUEST));
                assertThat(
                    ex.getMessage(),
                    is(
                        "Cannot send an inference request with audio, video or pdf inputs to an older node. "
                            + "Please wait until all nodes are upgraded before using audio, video or pdf inputs"
                    )
                );
            }
        }
    }

    /**
     * Verifies that URL-format inputs cannot be sent to nodes that do not support {@link InferenceString#URL_INPUT_FORMAT_SUPPORT_ADDED}.
     * <p>
     * We use an {@link DataType#IMAGE} instance rather than a randomly generated one to avoid interference from the earlier
     * {@link InferenceString#EMBEDDING_AUDIO_VIDEO_PDF_INPUT_SUPPORT_ADDED} gate: IMAGE pre-dates that gate and will not
     * trigger it, ensuring we always get the URL-specific error on any old node.
     */
    public void testUrlFormatIsNotBackwardsCompatible() throws IOException {
        assumeTrue("URL input format feature flag is not enabled", URL_INPUT_FORMAT_FEATURE_FLAG.isEnabled());
        var urlInstance = new InferenceString(DataType.IMAGE, DataFormat.URL, "https://example.com/image.png");
        var preUrlVersions = super.bwcVersions().stream().filter(v -> v.supports(URL_INPUT_FORMAT_SUPPORT_ADDED) == false).toList();
        for (var version : preUrlVersions) {
            var ex = assertThrows(
                ElasticsearchStatusException.class,
                () -> copyWriteable(urlInstance, getNamedWriteableRegistry(), instanceReader(), version)
            );
            assertThat(ex.status(), is(RestStatus.BAD_REQUEST));
            assertThat(
                ex.getMessage(),
                is(
                    "Cannot send an inference request with URL format inputs to an older node. "
                        + "Please wait until all nodes are upgraded before using URL format inputs"
                )
            );
        }
    }

    /**
     * Verifies that URL format is rejected when the feature flag is disabled. This test only runs in release builds (or when
     * the flag is explicitly disabled), since the flag is auto-enabled in snapshots.
     */
    public void testConstructorWithUrlFormat_rejectedWhenFeatureFlagDisabled() {
        assumeFalse("URL input format feature flag is enabled; skipping disabled-flag test", URL_INPUT_FORMAT_FEATURE_FLAG.isEnabled());
        var exception = assertThrows(
            IllegalArgumentException.class,
            () -> new InferenceString(DataType.IMAGE, DataFormat.URL, "https://example.com/image.png")
        );
        assertThat(exception.getMessage(), is("url format is not supported"));
    }

    public void testConstructorWithUrlFormat() {
        assumeTrue("URL input format feature flag is not enabled", URL_INPUT_FORMAT_FEATURE_FLAG.isEnabled());
        var nonTextTypes = new DataType[] { DataType.IMAGE, DataType.AUDIO, DataType.VIDEO, DataType.PDF };
        for (DataType type : nonTextTypes) {
            var inferenceString = new InferenceString(type, DataFormat.URL, "https://example.com/resource");
            assertThat(inferenceString.dataType(), is(type));
            assertThat(inferenceString.dataFormat(), is(DataFormat.URL));
            assertThat(inferenceString.value(), is("https://example.com/resource"));
        }
    }

    public void testConstructorWithUrlFormat_acceptsVariousSchemes() {
        assumeTrue("URL input format feature flag is not enabled", URL_INPUT_FORMAT_FEATURE_FLAG.isEnabled());
        var urls = List.of(
            "https://example.com/image.png",
            "http://example.com/audio.mp3",
            "s3://my-bucket/my-key/image.png",
            "gs://my-bucket/my-object",
            "az://my-container/my-blob"
        );
        urls.forEach(url -> new InferenceString(DataType.IMAGE, DataFormat.URL, url));
    }

    public void testConstructorWithUrlFormat_rejectsDataUri() {
        assumeTrue("URL input format feature flag is not enabled", URL_INPUT_FORMAT_FEATURE_FLAG.isEnabled());
        var exception = assertThrows(
            IllegalArgumentException.class,
            () -> new InferenceString(DataType.IMAGE, DataFormat.URL, "data:image/png;base64,abcd")
        );
        assertThat(exception.getMessage(), containsString("URL format inputs must not use the data URI scheme"));
    }

    public void testConstructorWithUrlFormat_rejectsInvalidUri() {
        assumeTrue("URL input format feature flag is not enabled", URL_INPUT_FORMAT_FEATURE_FLAG.isEnabled());
        var invalidUris = List.of("not a uri with spaces", "://missing-scheme");
        invalidUris.forEach(uri -> {
            var exception = assertThrows(IllegalArgumentException.class, () -> new InferenceString(DataType.IMAGE, DataFormat.URL, uri));
            assertThat(exception.getMessage(), containsString("URL format inputs must be valid URIs"));
        });
    }

    public void testParserWithUrlImage() throws IOException {
        assumeTrue("URL input format feature flag is not enabled", URL_INPUT_FORMAT_FEATURE_FLAG.isEnabled());
        testParserWithUrlFormat(DataType.IMAGE);
    }

    public void testParserWithUrlAudio() throws IOException {
        assumeTrue("URL input format feature flag is not enabled", URL_INPUT_FORMAT_FEATURE_FLAG.isEnabled());
        testParserWithUrlFormat(DataType.AUDIO);
    }

    public void testParserWithUrlVideo() throws IOException {
        assumeTrue("URL input format feature flag is not enabled", URL_INPUT_FORMAT_FEATURE_FLAG.isEnabled());
        testParserWithUrlFormat(DataType.VIDEO);
    }

    public void testParserWithUrlPdf() throws IOException {
        assumeTrue("URL input format feature flag is not enabled", URL_INPUT_FORMAT_FEATURE_FLAG.isEnabled());
        testParserWithUrlFormat(DataType.PDF);
    }

    private void testParserWithUrlFormat(DataType type) throws IOException {
        var url = "https://example.com/resource";
        var requestJson = Strings.format("""
            {
                "type": "%s",
                "format": "url",
                "value": "%s"
            }
            """, type.toString(), url);
        try (var parser = createParser(JsonXContent.jsonXContent, requestJson)) {
            var request = InferenceString.PARSER.apply(parser, null);
            assertThat(request.dataType(), is(type));
            assertThat(request.dataFormat(), is(DataFormat.URL));
            assertThat(request.value(), is(url));
        }
    }

    @Override
    protected Writeable.Reader<InferenceString> instanceReader() {
        return InferenceString::new;
    }

    @Override
    protected InferenceString createTestInstance() {
        return createRandom();
    }

    public static InferenceString createRandom() {
        return createRandomUsingDataTypes(EnumSet.allOf(DataType.class));
    }

    public static InferenceString createRandomUsingDataTypes(EnumSet<DataType> dataTypes) {
        DataType dataType = randomFrom(dataTypes);
        // Exclude URL format when the feature flag is disabled so that random generation does not attempt to
        // construct URL-format instances that would be rejected by the flag guard in InferenceString.
        var availableFormats = dataType.getSupportedFormats()
            .stream()
            .filter(f -> f != DataFormat.URL || URL_INPUT_FORMAT_FEATURE_FLAG.isEnabled())
            .collect(Collectors.toCollection(() -> EnumSet.noneOf(DataFormat.class)));
        DataFormat format = randomBoolean() ? randomFrom(availableFormats) : null;
        var value = convertToDataURIIfNeeded(dataType, format, randomAlphanumericOfLength(10));
        return new InferenceString(dataType, format, value);
    }

    // Ensure we create a valid value for the given format — a data URI for base64, an HTTPS URL for url, plain text otherwise
    public static String convertToDataURIIfNeeded(DataType dataType, DataFormat format, String value) {
        var formatToUse = format == null ? dataType.getDefaultFormat() : format;
        if (formatToUse == DataFormat.BASE64) {
            return "data:image/jpeg;base64," + value;
        } else if (formatToUse == DataFormat.URL) {
            return "https://example.com/" + value;
        }
        return value;
    }

    @Override
    protected InferenceString mutateInstance(InferenceString instance) throws IOException {
        if (randomBoolean()) {
            DataType newDataType = randomValueOtherThan(instance.dataType(), () -> randomFrom(DataType.values()));
            var availableFormats = newDataType.getSupportedFormats()
                .stream()
                .filter(f -> f != DataFormat.URL || URL_INPUT_FORMAT_FEATURE_FLAG.isEnabled())
                .collect(Collectors.toCollection(() -> EnumSet.noneOf(DataFormat.class)));
            DataFormat format = randomFrom(availableFormats);
            return new InferenceString(newDataType, format, convertToDataURIIfNeeded(newDataType, format, instance.value()));
        } else {
            String value = instance.value();
            return new InferenceString(
                instance.dataType(),
                instance.dataFormat(),
                randomValueOtherThan(
                    value,
                    () -> convertToDataURIIfNeeded(instance.dataType(), instance.dataFormat(), randomAlphanumericOfLength(10))
                )
            );
        }
    }

    @Override
    protected InferenceString mutateInstanceForVersion(InferenceString instance, TransportVersion version) {
        return instance;
    }

    @Override
    protected InferenceString doParseInstance(XContentParser parser) throws IOException {
        return InferenceString.PARSER.parse(parser, null);
    }

    /**
     * Converts the given {@link InferenceString} to a map matching what is sent in the request body. Equivalent to converting the
     * input to XContent, then parsing the XContent to a map.
     */
    public static Map<String, Object> toRequestMap(InferenceString input) {
        return Map.of(TYPE_FIELD, input.dataType().toString(), FORMAT_FIELD, input.dataFormat().toString(), VALUE_FIELD, input.value());
    }

    public static boolean isAudioVideoOrPdf(InferenceString testInstance) {
        return testInstance.isAudio() || testInstance.isVideo() || testInstance.isPdf();
    }

    public static DataType randomDataTypeSupportingBase64() {
        var dataTypesSupportingBase64 = Arrays.stream(DataType.values())
            .filter(type -> type.getSupportedFormats().contains(DataFormat.BASE64))
            .collect(Collectors.toSet());
        return randomFrom(dataTypesSupportingBase64);
    }

    public static String randomDataURI() {
        return TEST_DATA_URI + randomAlphanumericOfLength(5);
    }

    public static Map<String, Object> inferenceStringToMap(InferenceString inferenceString) {
        try {
            var builder = XContentFactory.contentBuilder(XContentType.JSON);
            inferenceString.toXContent(builder, null);
            return XContentHelper.convertToMap(BytesReference.bytes(builder), false, builder.contentType()).v2();
        } catch (IOException ioException) {
            throw new AssertionError("Exception when converting InferenceString to map", ioException);
        }
    }
}
