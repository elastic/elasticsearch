/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.profiling.action;

import org.elasticsearch.xcontent.ObjectPath;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.function.Consumer;

final class StackTrace implements ToXContentObject {
    private static final String[] PATH_FRAME_IDS = new String[] { "Stacktrace", "frame", "ids" };
    private static final String[] PATH_FRAME_TYPES = new String[] { "Stacktrace", "frame", "types" };

    static final int NATIVE_FRAME_TYPE = 3;
    static final int KERNEL_FRAME_TYPE = 4;
    int[] addressOrLines;
    String[] fileIds;
    String[] frameIds;
    int[] typeIds;
    SubGroup subGroups;
    double annualCO2Tons;
    double annualCostsUSD;
    long count;

    StackTrace(int[] addressOrLines, String[] fileIds, String[] frameIds, int[] typeIds) {
        this.addressOrLines = addressOrLines;
        this.fileIds = fileIds;
        this.frameIds = frameIds;
        this.typeIds = typeIds;
        annualCO2Tons = 0.0d;
        annualCostsUSD = 0.0d;
        count = 0;
    }

    private static final int BASE64_FRAME_ID_LENGTH = 32;

    private static final String SAFE_BASE64_ENCODER = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz01234456789-_";

    // tag::noformat
    private static final int[] SAFE_BASE64_DECODER = new int[] {
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 62, 0, 0, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 0, 0, 0, 0,
        0, 0, 0, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24,
        25, 0, 0, 0, 0, 63, 0, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44,
        45, 46, 47, 48, 49, 50, 51, 0, 0, 0, 0, 0
    };
    // end::noformat

    /**
     *
     * runLengthDecodeBase64Url decodes a run-length encoding for the base64-encoded input string.
     * E.g. the string 'BQADAg' is converted into an int array like [0, 0, 0, 0, 0, 2, 2, 2].
     * The motivating intent for this method is to unpack a base64-encoded run-length encoding
     * without using intermediate storage.
     *
     * This method relies on these assumptions and details:
     *  - array encoded using run-length and base64 always returns string of length 0, 3, or 6 (mod 8)
     *  - since original array is composed of int, we ignore Unicode codepoints
     *
     * @param input A base64-encoded string.
     * @param size Decoded length of the input.
     * @param capacity Capacity of the underlying array (>= size).
     *
     * @return Corresponding numbers that are encoded in the input.
     */
    // package-private for testing
    static int[] runLengthDecodeBase64Url(String input, int size, int capacity) {
        int[] output = new int[capacity];
        int multipleOf8 = size / 8;
        int remainder = size % 8;

        int n;
        int count;
        int value;
        int i;
        int j = 0;

        for (i = 0; i < multipleOf8 * 8; i += 8) {
            n = (charCodeAt(input, i) << 26) | (charCodeAt(input, i + 1) << 20) | (charCodeAt(input, i + 2) << 14) | (charCodeAt(
                input,
                i + 3
            ) << 8) | (charCodeAt(input, i + 4) << 2) | (charCodeAt(input, i + 5) >> 4);

            count = (n >> 24) & 0xff;
            value = (n >> 16) & 0xff;

            Arrays.fill(output, j, j + count, value);
            j += count;

            count = (n >> 8) & 0xff;
            value = n & 0xff;

            Arrays.fill(output, j, j + count, value);
            j += count;

            n = ((charCodeAt(input, i + 5) & 0xf) << 12) | (charCodeAt(input, i + 6) << 6) | charCodeAt(input, i + 7);

            count = (n >> 8) & 0xff;
            value = n & 0xff;

            Arrays.fill(output, j, j + count, value);
            j += count;
        }

        if (remainder == 6) {
            n = (charCodeAt(input, i) << 26) | (charCodeAt(input, i + 1) << 20) | (charCodeAt(input, i + 2) << 14) | (charCodeAt(
                input,
                i + 3
            ) << 8) | (charCodeAt(input, i + 4) << 2) | (charCodeAt(input, i + 5) >> 4);

            count = (n >> 24) & 0xff;
            value = (n >> 16) & 0xff;

            Arrays.fill(output, j, j + count, value);
            j += count;

            count = (n >> 8) & 0xff;
            value = n & 0xff;

            Arrays.fill(output, j, j + count, value);
        } else if (remainder == 3) {
            n = (charCodeAt(input, i) << 12) | (charCodeAt(input, i + 1) << 6) | charCodeAt(input, i + 2);
            n >>= 2;

            count = (n >> 8) & 0xff;
            value = n & 0xff;

            Arrays.fill(output, j, j + count, value);
        }
        return output;
    }

    // package-private for testing
    static int getAddressFromStackFrameID(String frameID) {
        int address = charCodeAt(frameID, 21) & 0xf;
        address <<= 6;
        address += charCodeAt(frameID, 22);
        address <<= 6;
        address += charCodeAt(frameID, 23);
        address <<= 6;
        address += charCodeAt(frameID, 24);
        address <<= 6;
        address += charCodeAt(frameID, 25);
        address <<= 6;
        address += charCodeAt(frameID, 26);
        address <<= 6;
        address += charCodeAt(frameID, 27);
        address <<= 6;
        address += charCodeAt(frameID, 28);
        address <<= 6;
        address += charCodeAt(frameID, 29);
        address <<= 6;
        address += charCodeAt(frameID, 30);
        address <<= 6;
        address += charCodeAt(frameID, 31);
        return address;
    }

    private static int charCodeAt(String input, int i) {
        return SAFE_BASE64_DECODER[input.charAt(i) & 0x7f];
    }

    // package-private for testing
    static String getFileIDFromStackFrameID(String frameID) {
        return frameID.substring(0, 21) + SAFE_BASE64_ENCODER.charAt(frameID.charAt(21) & 0x30);
    }

    public static StackTrace fromSource(Map<String, Object> source) {
        String inputFrameIDs = ObjectPath.eval(PATH_FRAME_IDS, source);
        if (inputFrameIDs == null) {
            // If synthetic source is disabled, fallback to dotted field names.
            inputFrameIDs = (String) source.get("Stacktrace.frame.ids");
        }
        String inputFrameTypes = ObjectPath.eval(PATH_FRAME_TYPES, source);
        if (inputFrameTypes == null) {
            // If synthetic source is disabled, fallback to dotted field names.
            inputFrameTypes = (String) source.get("Stacktrace.frame.types");
        }
        int countsFrameIDs = inputFrameIDs.length() / BASE64_FRAME_ID_LENGTH;

        String[] fileIDs = new String[countsFrameIDs];
        String[] frameIDs = new String[countsFrameIDs];
        int[] addressOrLines = new int[countsFrameIDs];

        // Step 1: Convert the base64-encoded frameID list into two separate
        // lists (frame IDs and file IDs), both of which are also base64-encoded.
        //
        // To get the frame ID, we grab the next 32 bytes.
        //
        // To get the file ID, we grab the first 22 bytes of the frame ID.
        // However, since the file ID is base64-encoded using 21.33 bytes
        // (16 * 4 / 3), then the 22 bytes have an extra 4 bits from the
        // address (see diagram in definition of EncodedStackTrace).
        for (int i = 0, pos = 0; i < countsFrameIDs; i++, pos += BASE64_FRAME_ID_LENGTH) {
            String frameID = inputFrameIDs.substring(pos, pos + BASE64_FRAME_ID_LENGTH);
            frameIDs[i] = frameID;
            fileIDs[i] = getFileIDFromStackFrameID(frameID);
            addressOrLines[i] = getAddressFromStackFrameID(frameID);
        }

        // Step 2: Convert the run-length byte encoding into a list of uint8s.
        int[] typeIDs = runLengthDecodeBase64Url(inputFrameTypes, inputFrameTypes.length(), countsFrameIDs);

        return new StackTrace(addressOrLines, fileIDs, frameIDs, typeIDs);
    }

    public void forNativeAndKernelFrames(Consumer<String> consumer) {
        for (int i = 0; i < this.fileIds.length; i++) {
            int frameType = this.typeIds[i];
            if (frameType == NATIVE_FRAME_TYPE || frameType == KERNEL_FRAME_TYPE) {
                consumer.accept(this.fileIds[i]);
            }
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject()
            .field("address_or_lines", this.addressOrLines)
            .field("file_ids", this.fileIds)
            .field("frame_ids", this.frameIds)
            .field("type_ids", this.typeIds)
            .field("annual_co2_tons", this.annualCO2Tons)
            .field("annual_costs_usd", this.annualCostsUSD)
            .field("count", this.count)
            .endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        StackTrace that = (StackTrace) o;
        return Arrays.equals(addressOrLines, that.addressOrLines)
            && Arrays.equals(fileIds, that.fileIds)
            && Arrays.equals(frameIds, that.frameIds)
            && Arrays.equals(typeIds, that.typeIds);
        // Don't compare metadata like annualized co2, annualized costs, subGroups and count.
    }

    // Don't hash metadata like annualized co2, annualized costs, subGroups and count.
    @Override
    public int hashCode() {
        int result = Arrays.hashCode(addressOrLines);
        result = 31 * result + Arrays.hashCode(fileIds);
        result = 31 * result + Arrays.hashCode(frameIds);
        result = 31 * result + Arrays.hashCode(typeIds);
        return result;
    }
}
