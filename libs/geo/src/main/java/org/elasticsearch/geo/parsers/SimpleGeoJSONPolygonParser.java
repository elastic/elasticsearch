/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.geo.parsers;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;

import org.elasticsearch.geo.geometry.Polygon;

/*
  We accept either a whole type: Feature, like this:

    { "type": "Feature",
      "geometry": {
         "type": "Polygon",
         "coordinates": [
           [ [100.0, 0.0], [101.0, 0.0], [101.0, 1.0],
             [100.0, 1.0], [100.0, 0.0] ]
           ]
       },
       "properties": {
         "prop0": "value0",
         "prop1": {"this": "that"}
         }
       }

   Or the inner object with type: Multi/Polygon.

   Or a type: FeatureCollection, if it has only one Feature which is a Polygon or MultiPolyon.

   type: MultiPolygon (union of polygons) is also accepted.
*/

/** Does minimal parsing of a GeoJSON object, to extract either Polygon or MultiPolygon, either directly as a the top-level type, or if
 *  the top-level type is Feature, as the geometry of that feature. */

@SuppressWarnings("unchecked")
public class SimpleGeoJSONPolygonParser {
  final String input;
  private int upto;
  private String polyType;
  private List<Object> coordinates;

  public SimpleGeoJSONPolygonParser(String input) {
    this.input = input;
  }

  public Polygon[] parse() throws ParseException {
    // parse entire object
    parseObject("");

    // make sure there's nothing left:
    readEnd();

    // The order of JSON object keys (type, geometry, coordinates in our case) can be arbitrary, so we wait until we are done parsing to
    // put the pieces together here:

    if (coordinates == null) {
      throw newParseException("did not see any polygon coordinates");
    }

    if (polyType == null) {
      throw newParseException("did not see type: Polygon or MultiPolygon");
    }

    if (polyType.equals("Polygon")) {
      return new Polygon[] {parsePolygon(coordinates)};
    } else {
      List<Polygon> polygons = new ArrayList<>();
      for(int i=0;i<coordinates.size();i++) {
        Object o = coordinates.get(i);
        if (o instanceof List == false) {
          throw newParseException("elements of coordinates array should be an array, but got: " + o.getClass());
        }
        polygons.add(parsePolygon((List<Object>) o));
      }

      return polygons.toArray(new Polygon[polygons.size()]);
    }
  }

  /** path is the "address" by keys of where we are, e.g. geometry.coordinates */
  private void parseObject(String path) throws ParseException {
    scan('{');
    boolean first = true;
    while (true) {
      char ch = peek();
      if (ch == '}') {
        break;
      } else if (first == false) {
        if (ch == ',') {
          // ok
          upto++;
          ch = peek();
          if (ch == '}') {
            break;
          }
        } else {
          throw newParseException("expected , but got " + ch);
        }
      }

      first = false;

      int uptoStart = upto;
      String key = parseString();

      if (path.equals("crs.properties") && key.equals("href")) {
        upto = uptoStart;
        throw newParseException("cannot handle linked crs");
      }

      scan(':');

      Object o;

      ch = peek();

      uptoStart = upto;

      if (ch == '[') {
        String newPath;
        if (path.length() == 0) {
          newPath = key;
        } else {
          newPath = path + "." + key;
        }
        o = parseArray(newPath);
      } else if (ch == '{') {
        String newPath;
        if (path.length() == 0) {
          newPath = key;
        } else {
          newPath = path + "." + key;
        }
        parseObject(newPath);
        o = null;
      } else if (ch == '"') {
        o = parseString();
      } else if (ch == 't') {
        scan("true");
        o = Boolean.TRUE;
      } else if (ch == 'f') {
        scan("false");
        o = Boolean.FALSE;
      } else if (ch == 'n') {
        scan("null");
        o = null;
      } else if (ch == '-' || ch == '.' || (ch >= '0' && ch <= '9')) {
        o = parseNumber();
      } else if (ch == '}') {
        break;
      } else {
        throw newParseException("expected array, object, string or literal value, but got: " + ch);
      }

      if (path.equals("crs.properties") && key.equals("name")) {
        if (o instanceof String == false) {
          upto = uptoStart;
          throw newParseException("crs.properties.name should be a string, but saw: " + o);
        }
        String crs = (String) o;
        if (crs.startsWith("urn:ogc:def:crs:OGC") == false || crs.endsWith(":CRS84") == false) {
          upto = uptoStart;
          throw newParseException("crs must be CRS84 from OGC, but saw: " + o);
        }
      }

      if (key.equals("type") && path.startsWith("crs") == false) {
        if (o instanceof String == false) {
          upto = uptoStart;
          throw newParseException("type should be a string, but got: " + o);
        }
        String type = (String) o;
        if (type.equals("Polygon") && isValidGeometryPath(path)) {
          polyType = "Polygon";
        } else if (type.equals("MultiPolygon") && isValidGeometryPath(path)) {
          polyType = "MultiPolygon";
        } else if ((type.equals("FeatureCollection") || type.equals("Feature")) && (path.equals("features.[]") || path.equals(""))) {
          // OK, we recurse
        } else {
          upto = uptoStart;
          throw newParseException("can only handle type FeatureCollection (if it has a single polygon geometry), Feature, Polygon or MutiPolygon, but got " + type);
        }
      } else if (key.equals("coordinates") && isValidGeometryPath(path)) {
        if (o instanceof List == false) {
          upto = uptoStart;
          throw newParseException("coordinates should be an array, but got: " + o.getClass());
        }
        if (coordinates != null) {
          upto = uptoStart;
          throw newParseException("only one Polygon or MultiPolygon is supported");
        }
        coordinates = (List<Object>) o;
      }
    }

    scan('}');
  }

  /** Returns true if the object path is a valid location to see a Multi/Polygon geometry */
  private boolean isValidGeometryPath(String path) {
    return path.equals("") || path.equals("geometry") || path.equals("features.[].geometry");
  }

  private Polygon parsePolygon(List<Object> coordinates) throws ParseException {
    List<Polygon> holes = new ArrayList<>();
    Object o = coordinates.get(0);
    if (o instanceof List == false) {
      throw newParseException("first element of polygon array must be an array [[lat, lon], [lat, lon] ...] but got: " + o);
    }
    double[][] polyPoints = parsePoints((List<Object>) o);
    for(int i=1;i<coordinates.size();i++) {
      o = coordinates.get(i);
      if (o instanceof List == false) {
        throw newParseException("elements of coordinates array must be an array [[lat, lon], [lat, lon] ...] but got: " + o);
      }
      double[][] holePoints = parsePoints((List<Object>) o);
      holes.add(new Polygon(holePoints[0], holePoints[1]));
    }
    return new Polygon(polyPoints[0], polyPoints[1], holes.toArray(new Polygon[holes.size()]));
  }

  /** Parses [[lat, lon], [lat, lon] ...] into 2d double array */
  private double[][] parsePoints(List<Object> o) throws ParseException {
    double[] lats = new double[o.size()];
    double[] lons = new double[o.size()];
    for(int i=0;i<o.size();i++) {
      Object point = o.get(i);
      if (point instanceof List == false) {
        throw newParseException("elements of coordinates array must [lat, lon] array, but got: " + point);
      }
      List<Object> pointList = (List<Object>) point;
      if (pointList.size() != 2) {
        throw newParseException("elements of coordinates array must [lat, lon] array, but got wrong element count: " + pointList);
      }
      if (pointList.get(0) instanceof Double == false) {
        throw newParseException("elements of coordinates array must [lat, lon] array, but first element is not a Double: " + pointList.get(0));
      }
      if (pointList.get(1) instanceof Double == false) {
        throw newParseException("elements of coordinates array must [lat, lon] array, but second element is not a Double: " + pointList.get(1));
      }

      // lon, lat ordering in GeoJSON!
      lons[i] = ((Double) pointList.get(0)).doubleValue();
      lats[i] = ((Double) pointList.get(1)).doubleValue();
    }

    return new double[][] {lats, lons};
  }

  private List<Object> parseArray(String path) throws ParseException {
    List<Object> result = new ArrayList<>();
    scan('[');
    while (upto < input.length()) {
      char ch = peek();
      if (ch == ']') {
        scan(']');
        return result;
      }

      if (result.size() > 0) {
        if (ch != ',') {
          throw newParseException("expected ',' separating list items, but got '" + ch + "'");
        }

        // skip the ,
        upto++;

        if (upto == input.length()) {
          throw newParseException("hit EOF while parsing array");
        }
        ch = peek();
      }

      Object o;
      if (ch == '[') {
        o = parseArray(path + ".[]");
      } else if (ch == '{') {
        // This is only used when parsing the "features" in type: FeatureCollection
        parseObject(path + ".[]");
        o = null;
      } else if (ch == '-' || ch == '.' || (ch >= '0' && ch <= '9')) {
        o = parseNumber();
      } else {
        throw newParseException("expected another array or number while parsing array, not '" + ch + "'");
      }

      result.add(o);
    }

    throw newParseException("hit EOF while reading array");
  }

  private Number parseNumber() throws ParseException {
    StringBuilder b = new StringBuilder();
    int uptoStart = upto;
    while (upto < input.length()) {
      char ch = input.charAt(upto);
      if (ch == '-' || ch == '.' || (ch >= '0' && ch <= '9') || ch == 'e' || ch == 'E') {
        upto++;
        b.append(ch);
      } else {
        break;
      }
    }

    // we only handle doubles
    try {
      return Double.parseDouble(b.toString());
    } catch (NumberFormatException nfe) {
      upto = uptoStart;
      throw newParseException("could not parse number as double");
    }
  }

  private String parseString() throws ParseException {
    scan('"');
    StringBuilder b = new StringBuilder();
    while (upto < input.length()) {
      char ch = input.charAt(upto);
      if (ch == '"') {
        upto++;
        return b.toString();
      }
      if (ch == '\\') {
        // an escaped character
        upto++;
        if (upto == input.length()) {
          throw newParseException("hit EOF inside string literal");
        }
        ch = input.charAt(upto);
        if (ch == 'u') {
          // 4 hex digit unicode BMP escape
          upto++;
          if (upto + 4 > input.length()) {
            throw newParseException("hit EOF inside string literal");
          }
          b.append(Integer.parseInt(input.substring(upto, upto+4), 16));
        } else if (ch == '\\') {
          b.append('\\');
          upto++;
        } else {
          // TODO: allow \n, \t, etc.???
          throw newParseException("unsupported string escape character \\" + ch);
        }
      } else {
        b.append(ch);
        upto++;
      }
    }

    throw newParseException("hit EOF inside string literal");
  }

  private char peek() throws ParseException {
    while (upto < input.length()) {
      char ch = input.charAt(upto);
      if (isJSONWhitespace(ch)) {
        upto++;
        continue;
      }
      return ch;
    }

    throw newParseException("unexpected EOF");
  }

  /** Scans across whitespace and consumes the expected character, or throws {@code ParseException} if the character is wrong */
  private void scan(char expected) throws ParseException {
    while (upto < input.length()) {
      char ch = input.charAt(upto);
      if (isJSONWhitespace(ch)) {
        upto++;
        continue;
      }
      if (ch != expected) {
        throw newParseException("expected '" + expected + "' but got '" + ch + "'");
      }
      upto++;
      return;
    }
    throw newParseException("expected '" + expected + "' but got EOF");
  }

  private void readEnd() throws ParseException {
    while (upto < input.length()) {
      char ch = input.charAt(upto);
      if (isJSONWhitespace(ch) == false) {
        throw newParseException("unexpected character '" + ch + "' after end of GeoJSON object");
      }
      upto++;
    }
  }

  /** Scans the expected string, or throws {@code ParseException} */
  private void scan(String expected) throws ParseException {
    if (upto + expected.length() > input.length()) {
      throw newParseException("expected \"" + expected + "\" but hit EOF");
    }
    String subString = input.substring(upto, upto+expected.length());
    if (subString.equals(expected) == false) {
      throw newParseException("expected \"" + expected + "\" but got \"" + subString + "\"");
    }
    upto += expected.length();
  }

  private static boolean isJSONWhitespace(char ch) {
    // JSON doesn't accept allow unicode whitespace?
    return ch == 0x20 || // space
      ch == 0x09 || // tab
      ch == 0x0a || // line feed
      ch == 0x0d;  // newline
  }

  /** When calling this, upto should be at the position of the incorrect character! */
  private ParseException newParseException(String details) throws ParseException {
    String fragment;
    int end = Math.min(input.length(), upto+1);
    if (upto < 50) {
      fragment = input.substring(0, end);
    } else {
      fragment = "..." + input.substring(upto-50, end);
    }
    return new ParseException(details + " at character offset " + upto + "; fragment leading to this:\n" + fragment, upto);
  }
}
