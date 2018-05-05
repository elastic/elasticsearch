// Copyright 2014 Google Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.google.openlocationcode;

import java.math.BigDecimal;
import java.util.Locale;

/**
 * Convert locations to and from convenient short codes.
 *
 * Open Location Codes are short, ~10 character codes that can be used instead of street
 * addresses. The codes can be generated and decoded offline, and use a reduced character set that
 * minimises the chance of codes including words.
 *
 * This provides both object and static methods.
 *
 * Create an object with:
 * OpenLocationCode code = new OpenLocationCode("7JVW52GR+2V");
 * OpenLocationCode code = new OpenLocationCode("52GR+2V");
 * OpenLocationCode code = new OpenLocationCode(27.175063, 78.042188);
 * OpenLocationCode code = new OpenLocationCode(27.175063, 78.042188, 11);
 *
 * Once you have a code object, you can apply the other methods to it, such as to shorten:
 * code.shorten(27.176, 78.05)
 *
 * Recover the nearest match (if the code was a short code):
 * code.recover(27.176, 78.05)
 *
 * Or decode a code into its coordinates, returning a CodeArea object.
 * code.decode()
 *
 * @author Jiri Semecky
 * @author Doug Rinckes
 */
public final class OpenLocationCode {

  // Provides a normal precision code, approximately 14x14 meters.
  public static final int CODE_PRECISION_NORMAL = 10;

  // Provides an extra precision code, approximately 2x3 meters.
  public static final int CODE_PRECISION_EXTRA = 11;

  // A separator used to break the code into two parts to aid memorability.
  private static final char SEPARATOR = '+';

  // The number of characters to place before the separator.
  private static final int SEPARATOR_POSITION = 8;

  // The character used to pad codes.
  private static final char PADDING_CHARACTER = '0';

  // The character set used to encode the values.
  private static final String CODE_ALPHABET = "23456789CFGHJMPQRVWX";

  // Note: The double type can't be used because of the rounding arithmetic due to floating point
  // implementation. Eg. "8.95 - 8" can give result 0.9499999999999 instead of 0.95 which
  // incorrectly classify the points on the border of a cell. Therefore all the calculation is done
  // using BigDecimal.

  // The base to use to convert numbers to/from.
  private static final BigDecimal ENCODING_BASE = new BigDecimal(CODE_ALPHABET.length());

  // The maximum value for latitude in degrees.
  private static final BigDecimal LATITUDE_MAX = new BigDecimal(90);

  // The maximum value for longitude in degrees.
  private static final BigDecimal LONGITUDE_MAX = new BigDecimal(180);

  // Maximum code length using just lat/lng pair encoding.
  private static final int PAIR_CODE_LENGTH = 10;

  // Number of columns in the grid refinement method.
  private static final BigDecimal GRID_COLUMNS = new BigDecimal(4);

  // Number of rows in the grid refinement method.
  private static final BigDecimal GRID_ROWS = new BigDecimal(5);

  /**
   * Coordinates of a decoded Open Location Code.
   *
   * <p>The coordinates include the latitude and longitude of the lower left and upper right corners
   * and the center of the bounding box for the area the code represents.
   */
 public static class CodeArea {

    private final BigDecimal southLatitude;
    private final BigDecimal westLongitude;
    private final BigDecimal northLatitude;
    private final BigDecimal eastLongitude;

    public CodeArea(
        BigDecimal southLatitude,
        BigDecimal westLongitude,
        BigDecimal northLatitude,
        BigDecimal eastLongitude) {
      this.southLatitude = southLatitude;
      this.westLongitude = westLongitude;
      this.northLatitude = northLatitude;
      this.eastLongitude = eastLongitude;
    }

    public double getSouthLatitude() {
      return southLatitude.doubleValue();
    }

    public double getWestLongitude() {
      return westLongitude.doubleValue();
    }

    public double getLatitudeHeight() {
      return northLatitude.subtract(southLatitude).doubleValue();
    }

    public double getLongitudeWidth() {
      return eastLongitude.subtract(westLongitude).doubleValue();
    }

    public double getCenterLatitude() {
      return southLatitude.add(northLatitude).doubleValue() / 2;
    }

    public double getCenterLongitude() {
      return westLongitude.add(eastLongitude).doubleValue() / 2;
    }

    public double getNorthLatitude() {
      return northLatitude.doubleValue();
    }

    public double getEastLongitude() {
      return eastLongitude.doubleValue();
    }
  }

  /** The current code for objects. */
  private final String code;

  /**
   * Creates Open Location Code object for the provided code.
   * @param code A valid OLC code. Can be a full code or a shortened code.
   * @throws IllegalArgumentException when the passed code is not valid.
  */
  public OpenLocationCode(String code) throws IllegalArgumentException {
    if (!isValidCode(code.toUpperCase(Locale.ROOT))) {
      throw new IllegalArgumentException(
          "The provided code '" + code + "' is not a valid Open Location Code.");
    }
    this.code = code.toUpperCase(Locale.ROOT);
  }

  /**
   * Creates Open Location Code.
   * @param latitude The latitude in decimal degrees.
   * @param longitude The longitude in decimal degrees.
   * @param codeLength The desired number of digits in the code.
   * @throws IllegalArgumentException if the code length is not valid.
   */
  public OpenLocationCode(double latitude, double longitude, int codeLength)
      throws IllegalArgumentException {
    // Check that the code length requested is valid.
    if (codeLength < 4 || (codeLength < PAIR_CODE_LENGTH && codeLength % 2 == 1)) {
      throw new IllegalArgumentException("Illegal code length " + codeLength);
    }
    // Ensure that latitude and longitude are valid.
    latitude = clipLatitude(latitude);
    longitude = normalizeLongitude(longitude);

    // Latitude 90 needs to be adjusted to be just less, so the returned code can also be decoded.
    if (latitude == LATITUDE_MAX.doubleValue()) {
      latitude = latitude - 0.9 * computeLatitudePrecision(codeLength);
    }

    // Adjust latitude and longitude to be in positive number ranges.
    BigDecimal remainingLatitude = new BigDecimal(latitude).add(LATITUDE_MAX);
    BigDecimal remainingLongitude = new BigDecimal(longitude).add(LONGITUDE_MAX);

    // Count how many digits have been created.
    int generatedDigits = 0;
    // Store the code.
    StringBuilder codeBuilder = new StringBuilder();
    // The precisions are initially set to ENCODING_BASE^2 because they will be immediately
    // divided.
    BigDecimal latPrecision = ENCODING_BASE.multiply(ENCODING_BASE);
    BigDecimal lngPrecision = ENCODING_BASE.multiply(ENCODING_BASE);
    while (generatedDigits < codeLength) {
      if (generatedDigits < PAIR_CODE_LENGTH) {
        // Use the normal algorithm for the first set of digits.
        latPrecision = latPrecision.divide(ENCODING_BASE);
        lngPrecision = lngPrecision.divide(ENCODING_BASE);
        BigDecimal latDigit = remainingLatitude.divide(latPrecision, 0, BigDecimal.ROUND_FLOOR);
        BigDecimal lngDigit = remainingLongitude.divide(lngPrecision, 0, BigDecimal.ROUND_FLOOR);
        remainingLatitude = remainingLatitude.subtract(latPrecision.multiply(latDigit));
        remainingLongitude = remainingLongitude.subtract(lngPrecision.multiply(lngDigit));
        codeBuilder.append(CODE_ALPHABET.charAt(latDigit.intValue()));
        codeBuilder.append(CODE_ALPHABET.charAt(lngDigit.intValue()));
        generatedDigits += 2;
      } else {
        // Use the 4x5 grid for remaining digits.
        latPrecision = latPrecision.divide(GRID_ROWS);
        lngPrecision = lngPrecision.divide(GRID_COLUMNS);
        BigDecimal row = remainingLatitude.divide(latPrecision, 0, BigDecimal.ROUND_FLOOR);
        BigDecimal col = remainingLongitude.divide(lngPrecision, 0, BigDecimal.ROUND_FLOOR);
        remainingLatitude = remainingLatitude.subtract(latPrecision.multiply(row));
        remainingLongitude = remainingLongitude.subtract(lngPrecision.multiply(col));
        codeBuilder.append(
            CODE_ALPHABET.charAt(row.intValue() * GRID_COLUMNS.intValue() + col.intValue()));
        generatedDigits += 1;
      }
      // If we are at the separator position, add the separator.
      if (generatedDigits == SEPARATOR_POSITION) {
        codeBuilder.append(SEPARATOR);
      }
    }
    // If the generated code is shorter than the separator position, pad the code and add the
    // separator.
    if (generatedDigits < SEPARATOR_POSITION) {
      for (; generatedDigits < SEPARATOR_POSITION; generatedDigits++) {
        codeBuilder.append(PADDING_CHARACTER);
      }
      codeBuilder.append(SEPARATOR);
    }
    this.code = codeBuilder.toString();
  }

  /**
   * Creates Open Location Code with the default precision length.
   * @param latitude The latitude in decimal degrees.
   * @param longitude The longitude in decimal degrees.
   */
  public OpenLocationCode(double latitude, double longitude) {
    this(latitude, longitude, CODE_PRECISION_NORMAL);
  }

  /**
   * Returns the string representation of the code.
   */
  public String getCode() {
    return code;
  }

  /**
   * Encodes latitude/longitude into 10 digit Open Location Code. This method is equivalent to
   * creating the OpenLocationCode object and getting the code from it.
   * @param latitude The latitude in decimal degrees.
   * @param longitude The longitude in decimal degrees.
   */
  public static String encode(double latitude, double longitude) {
    return new OpenLocationCode(latitude, longitude).getCode();
  }

  /**
   * Encodes latitude/longitude into Open Location Code of the provided length. This method is
   * equivalent to creating the OpenLocationCode object and getting the code from it.
   * @param latitude The latitude in decimal degrees.
   * @param longitude The longitude in decimal degrees.
   */
  public static String encode(double latitude, double longitude, int codeLength) {
    return new OpenLocationCode(latitude, longitude, codeLength).getCode();
  }

  /**
   * Decodes {@link OpenLocationCode} object into {@link CodeArea} object encapsulating
   * latitude/longitude bounding box.
   */
  public CodeArea decode() {
    if (!isFullCode(code)) {
      throw new IllegalStateException(
          "Method decode() could only be called on valid full codes, code was " + code + ".");
    }
    // Strip padding and separator characters out of the code.
    String decoded = code.replace(String.valueOf(SEPARATOR), "")
        .replace(String.valueOf(PADDING_CHARACTER), "");

    int digit = 0;
    // The precisions are initially set to ENCODING_BASE^2 because they will be immediately
    // divided.
    BigDecimal latPrecision = ENCODING_BASE.multiply(ENCODING_BASE);
    BigDecimal lngPrecision = ENCODING_BASE.multiply(ENCODING_BASE);
    // Save the coordinates.
    BigDecimal southLatitude = new BigDecimal(0);
    BigDecimal westLongitude = new BigDecimal(0);

    // Decode the digits.
    while (digit < decoded.length()) {
      if (digit < PAIR_CODE_LENGTH) {
        // Decode a pair of digits, the first being latitude and the second being longitude.
        latPrecision = latPrecision.divide(ENCODING_BASE);
        lngPrecision = lngPrecision.divide(ENCODING_BASE);
        int digitVal = CODE_ALPHABET.indexOf(decoded.charAt(digit));
        southLatitude = southLatitude.add(latPrecision.multiply(new BigDecimal(digitVal)));
        digitVal = CODE_ALPHABET.indexOf(decoded.charAt(digit + 1));
        westLongitude = westLongitude.add(lngPrecision.multiply(new BigDecimal(digitVal)));
        digit += 2;
      } else {
        // Use the 4x5 grid for digits after 10.
        int digitVal = CODE_ALPHABET.indexOf(decoded.charAt(digit));
        int row = (int) (digitVal / GRID_COLUMNS.intValue());
        int col = digitVal % GRID_COLUMNS.intValue();
        latPrecision = latPrecision.divide(GRID_ROWS);
        lngPrecision = lngPrecision.divide(GRID_COLUMNS);
        southLatitude = southLatitude.add(latPrecision.multiply(new BigDecimal(row)));
        westLongitude = westLongitude.add(lngPrecision.multiply(new BigDecimal(col)));
        digit += 1;
      }
    }
    return new CodeArea(
        southLatitude.subtract(LATITUDE_MAX),
        westLongitude.subtract(LONGITUDE_MAX),
        southLatitude.subtract(LATITUDE_MAX).add(latPrecision),
        westLongitude.subtract(LONGITUDE_MAX).add(lngPrecision));
  }

  /**
   * Decodes code representing Open Location Code into {@link CodeArea} object encapsulating
   * latitude/longitude bounding box.
   *
   * @param code Open Location Code to be decoded.
   * @throws IllegalArgumentException if the provided code is not a valid Open Location Code.
   */
  public static CodeArea decode(String code) throws IllegalArgumentException {
    return new OpenLocationCode(code).decode();
  }

  /** Returns whether this {@link OpenLocationCode} is a full Open Location Code. */
  public boolean isFull() {
    return code.indexOf(SEPARATOR) == SEPARATOR_POSITION;
  }

  /** Returns whether the provided Open Location Code is a full Open Location Code. */
  public static boolean isFull(String code) throws IllegalArgumentException {
    return new OpenLocationCode(code).isFull();
  }

  /** Returns whether this {@link OpenLocationCode} is a short Open Location Code. */
  public boolean isShort() {
    return code.indexOf(SEPARATOR) >= 0 && code.indexOf(SEPARATOR) < SEPARATOR_POSITION;
  }

  /** Returns whether the provided Open Location Code is a short Open Location Code. */
  public static boolean isShort(String code) throws IllegalArgumentException {
    return new OpenLocationCode(code).isShort();
  }

  /**
   * Returns whether this {@link OpenLocationCode} is a padded Open Location Code, meaning that it
   * contains less than 8 valid digits.
   */
  private boolean isPadded() {
    return code.indexOf(PADDING_CHARACTER) >= 0;
  }

  /**
   * Returns whether the provided Open Location Code is a padded Open Location Code, meaning that it
   * contains less than 8 valid digits.
   */
  public static boolean isPadded(String code) throws IllegalArgumentException {
    return new OpenLocationCode(code).isPadded();
  }

  /**
   * Returns short {@link OpenLocationCode} from the full Open Location Code created by removing
   * four or six digits, depending on the provided reference point. It removes as many digits as
   * possible.
   */
  public OpenLocationCode shorten(double referenceLatitude, double referenceLongitude) {
    if (!isFull()) {
      throw new IllegalStateException("shorten() method could only be called on a full code.");
    }
    if (isPadded()) {
      throw new IllegalStateException("shorten() method can not be called on a padded code.");
    }

    CodeArea codeArea = decode();
    double range = Math.max(
        Math.abs(referenceLatitude - codeArea.getCenterLatitude()),
        Math.abs(referenceLongitude - codeArea.getCenterLongitude()));
    // We are going to check to see if we can remove three pairs, two pairs or just one pair of
    // digits from the code.
    for (int i = 4; i >= 1; i--) {
      // Check if we're close enough to shorten. The range must be less than 1/2
      // the precision to shorten at all, and we want to allow some safety, so
      // use 0.3 instead of 0.5 as a multiplier.
      if (range < (computeLatitudePrecision(i * 2) * 0.3)) {
        // We're done.
        return new OpenLocationCode(code.substring(i * 2));
      }
    }
    throw new IllegalArgumentException(
        "Reference location is too far from the Open Location Code center.");
  }

  /**
   * Returns an {@link OpenLocationCode} object representing a full Open Location Code from this
   * (short) Open Location Code, given the reference location.
   */
  public OpenLocationCode recover(double referenceLatitude, double referenceLongitude) {
    if (isFull()) {
      // Note: each code is either full xor short, no other option.
      return this;
    }
    referenceLatitude = clipLatitude(referenceLatitude);
    referenceLongitude = normalizeLongitude(referenceLongitude);

    int digitsToRecover = SEPARATOR_POSITION - code.indexOf(SEPARATOR);
    // The precision (height and width) of the missing prefix in degrees.
    double prefixPrecision = Math.pow(ENCODING_BASE.intValue(), 2 - (digitsToRecover / 2));

    // Use the reference location to generate the prefix.
    String recoveredPrefix =
        new OpenLocationCode(referenceLatitude, referenceLongitude)
            .getCode()
            .substring(0, digitsToRecover);
    // Combine the prefix with the short code and decode it.
    OpenLocationCode recovered = new OpenLocationCode(recoveredPrefix + code);
    CodeArea recoveredCodeArea = recovered.decode();
    // Work out whether the new code area is too far from the reference location. If it is, we
    // move it. It can only be out by a single precision step.
    double recoveredLatitude = recoveredCodeArea.getCenterLatitude();
    double recoveredLongitude = recoveredCodeArea.getCenterLongitude();

    // Move the recovered latitude by one precision up or down if it is too far from the reference,
    // unless doing so would lead to an invalid latitude.
    double latitudeDiff = recoveredLatitude - referenceLatitude;
    if (latitudeDiff > prefixPrecision / 2
            && recoveredLatitude - prefixPrecision > -LATITUDE_MAX.intValue()) {
      recoveredLatitude -= prefixPrecision;
    } else if (latitudeDiff < -prefixPrecision / 2
            && recoveredLatitude + prefixPrecision < LATITUDE_MAX.intValue()) {
      recoveredLatitude += prefixPrecision;
    }

    // Move the recovered longitude by one precision up or down if it is too far from the
    // reference.
    double longitudeDiff = recoveredCodeArea.getCenterLongitude() - referenceLongitude;
    if (longitudeDiff > prefixPrecision / 2) {
      recoveredLongitude -= prefixPrecision;
    } else if (longitudeDiff < -prefixPrecision / 2) {
      recoveredLongitude += prefixPrecision;
    }

    return new OpenLocationCode(
        recoveredLatitude, recoveredLongitude, recovered.getCode().length() - 1);
  }

  /**
   * Returns whether the bounding box specified by the Open Location Code contains provided point.
   */
  public boolean contains(double latitude, double longitude) {
    CodeArea codeArea = decode();
    return codeArea.getSouthLatitude() <= latitude
        && latitude < codeArea.getNorthLatitude()
        && codeArea.getWestLongitude() <= longitude
        && longitude < codeArea.getEastLongitude();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    OpenLocationCode that = (OpenLocationCode) o;
    return hashCode() == that.hashCode();
  }

  @Override
  public int hashCode() {
    return code != null ? code.hashCode() : 0;
  }

  @Override
  public String toString() {
    return getCode();
  }

  // Exposed static helper methods.

  /** Returns whether the provided string is a valid Open Location code. */
  public static boolean isValidCode(String code) {
    if (code == null || code.length() < 2) {
      return false;
    }
    code = code.toUpperCase(Locale.ROOT);

    // There must be exactly one separator.
    int separatorPosition = code.indexOf(SEPARATOR);
    if (separatorPosition == -1) {
      return false;
    }
    if (separatorPosition != code.lastIndexOf(SEPARATOR)) {
      return false;
    }

    if (separatorPosition % 2 != 0) {
      return false;
    }

    // Check first two characters: only some values from the alphabet are permitted.
    if (separatorPosition == 8) {
      // First latitude character can only have first 9 values.
      Integer index0 = CODE_ALPHABET.indexOf(code.charAt(0));
      if (index0 == null || index0 > 8) {
        return false;
      }

      // First longitude character can only have first 18 values.
      Integer index1 = CODE_ALPHABET.indexOf(code.charAt(1));
      if (index1 == null || index1 > 17) {
        return false;
      }
    }

    // Check the characters before the separator.
    boolean paddingStarted = false;
    for (int i = 0; i < separatorPosition; i++) {
      if (paddingStarted) {
        // Once padding starts, there must not be anything but padding.
        if (code.charAt(i) != PADDING_CHARACTER) {
          return false;
        }
        continue;
      }
      if (CODE_ALPHABET.indexOf(code.charAt(i)) != -1) {
        continue;
      }
      if (PADDING_CHARACTER == code.charAt(i)) {
        paddingStarted = true;
        // Padding can start on even character: 2, 4 or 6.
        if (i != 2 && i != 4 && i != 6) {
          return false;
        }
        continue;
      }
      return false; // Illegal character.
    }

    // Check the characters after the separator.
    if (code.length() > separatorPosition + 1) {
      if (paddingStarted) {
        return false;
      }
      // Only one character after separator is forbidden.
      if (code.length() == separatorPosition + 2) {
        return false;
      }
      for (int i = separatorPosition + 1; i < code.length(); i++) {
        if (CODE_ALPHABET.indexOf(code.charAt(i)) == -1) {
          return false;
        }
      }
    }

    return true;
  }

  /** Returns if the code is a valid full Open Location Code. */
  public static boolean isFullCode(String code) {
    try {
      return new OpenLocationCode(code).isFull();
    } catch (IllegalArgumentException e) {
      return false;
    }
  }

  /** Returns if the code is a valid short Open Location Code. */
  public static boolean isShortCode(String code) {
    try {
      return new OpenLocationCode(code).isShort();
    } catch (IllegalArgumentException e) {
      return false;
    }
  }

  // Private static methods.

  private static double clipLatitude(double latitude) {
    return Math.min(Math.max(latitude, -LATITUDE_MAX.intValue()), LATITUDE_MAX.intValue());
  }

  private static double normalizeLongitude(double longitude) {
    while (longitude < -LONGITUDE_MAX.intValue()) {
      longitude = longitude + LONGITUDE_MAX.intValue() * 2;
    }
    while (longitude >= LONGITUDE_MAX.intValue()) {
      longitude = longitude - LONGITUDE_MAX.intValue() * 2;
    }
    return longitude;
  }

  /**
   * Compute the latitude precision value for a given code length. Lengths <= 10 have the same
   * precision for latitude and longitude, but lengths > 10 have different precisions due to the
   * grid method having fewer columns than rows. Copied from the JS implementation.
   */
  private static double computeLatitudePrecision(int codeLength) {
    if (codeLength <= CODE_PRECISION_NORMAL) {
      return Math.pow(ENCODING_BASE.intValue(), Math.floor(codeLength / -2 + 2));
    }
    return Math.pow(ENCODING_BASE.intValue(), -3)
        / Math.pow(GRID_ROWS.intValue(), codeLength - PAIR_CODE_LENGTH);
  }
}
