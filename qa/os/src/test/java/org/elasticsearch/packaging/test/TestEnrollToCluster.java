/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.packaging.test;

import org.elasticsearch.Version;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.ssl.PemUtils;
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.packaging.util.Archives;
import org.elasticsearch.packaging.util.Installation;
import org.elasticsearch.packaging.util.Shell;
import org.elasticsearch.test.http.MockRequest;
import org.elasticsearch.test.http.MockResponse;
import org.elasticsearch.test.http.MockWebServer;
import org.elasticsearch.xpack.core.security.EnrollmentToken;
import org.elasticsearch.xpack.core.ssl.TestsSSLService;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;

import java.nio.file.Files;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import static org.elasticsearch.packaging.util.Archives.installArchive;
import static org.elasticsearch.packaging.util.Archives.verifyArchiveInstallation;
import static org.elasticsearch.packaging.util.FileMatcher.Fileness.Directory;
import static org.elasticsearch.packaging.util.FileMatcher.Fileness.File;
import static org.elasticsearch.packaging.util.FileMatcher.file;
import static org.elasticsearch.packaging.util.FileMatcher.p660;
import static org.elasticsearch.packaging.util.FileMatcher.p750;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;
import static org.junit.Assume.assumeTrue;

public class TestEnrollToCluster extends PackagingTestCase {

    private static MockWebServer esNode;

    @BeforeClass
    public static void filterDistros() {
        assumeTrue("only archives", distribution.isArchive());
    }

    /**
     * We mock an enrollment response from an elasticsearch node. What we want to test in this class is that the startup scripts
     * can correctly parse the parameters from the command line and that the enrollment can be invoked as expected. The enrollment
     * process is tested in other tests so we don't care about duplicating this here and attempt to spin up multiple nodes and
     * do a proper enrollment.
     * We do need to have TLS because the enrolling process is hardcoded to work only with HTTP over TLS and to trust certificates
     * that are signed by the certificate identified with the fingerprint in the enrollment token.
     */
    @Before
    public void setupMockEsNode() throws Exception {
        String enrollmentResponseBody = "{\"nodes_addresses\"=[\"127.0.0.1:9300\", \"192.168.1.10:9301\"], "
            + "\"http_ca_key\"=\"MIIJQQIBADANBgkqhkiG9w0BAQEFAASCCSswggknAgEAAoICAQDZ3Iln8S81Y80z4DKVo8+UbB8hBwHSr17BE/YuiIFthoCne9T2"
            + "1Kg1h48vCqAYH8gSZHiQYUVwqbuoLbdoHvPEDmqPPAJGbnyTHum1O1aEEduALShfsFUZ5PQH8Hx4ycqLV/63iIYvb2If0pAEyzZ95qzn7SF7F2Qus1L20h"
            + "mU9ZVA+ogcsOIui5OT21fNrUcKxAGM1KuZx0pK0aBG5mMLLJ5VbO/rSuetc/9EbKSnmW/LvDm1arBoEI9stMBA6fcLKK95JV7n5eWyCyLBjdGCWNZNn172"
            + "BmxY4FS4dKMLtKoKUTaWrPgJgrvrp+G7e+9o9f3eBuP4+ja6pjTlxcMR+pzF2TWGZn+oyey3Uy15djuuMNbZINXui/bwTEK5X43vx1QHEaTGiwbUovLHqm"
            + "w5hHY6uWaLV7zCisuy7V2HWVKBIAf/78fvukNzohkxQ08FxMLDOvzIruwAQ/Yu4aGcZcKxTUDdQhEJaYjsEQ7edf5UnTPj+gSHGihWqxHkGtwTtnat0zcV"
            + "othNwn9HX2TpojPr2H3NpzeJcnlKDYGmAY8/ENxZk+vz3F0pW5jyWZOYwx2CctoZLdYEAsI2QTwLH93rLDRF1Fl3rp1px8XwfKkJS7Dfqjt6qaI02yiYS/"
            + "YkkfouPEKR3PFEY0wXq2EzBlj1ovbWlV9PgCK0DhnQZwIDAQABAoICAB/9QPPRN0RYpi3i0qqkRfueMKfx1nOwKnKhUrmcc5y4bjWpeijQKu7JO94FamKA"
            + "cCk7NXTFw6N6WMKmC9MvEE4Oa9kiT5c16/bSSDDDSL3VvWxBtTbvtl85/hcYWb6GqsXxIsaiNknKyhawHVOG3zZ3Y5YefJcNZTlyPVFeokD8GnFTGB9WXa"
            + "/F8OJ6B5d8xPywsSWTqKrI14DK7QTcGVjvsUQL3eKnugL/EFFkvnyZjA/XUIXx53swS08D72LYt9ycmb9pHFliqWqONglDoKrKDpWRPClV4hzeu7Hl9nbm"
            + "jT08lY5kUXtcBenhWcTkus+npyIt0tWhL94SP5wpgK3ivbNsR+rfyETKC44B16wLdq3qyDc7DRPC2qAXriCqhEzroJeUitX9xP4OT9Ryi5SrHBDbs4vd5n"
            + "Jv1v0+syvu7DBRvCaYgFU9brennYFLkuzxCdYWt8F4sbQc/LtqeVPNfHa0syMnAT8q1GVFD5HbTZ6/Gat19yu3AMCtRQWD1x56hzM1oYOde6sYardvKNYx"
            + "+sp6GCgr3eJ/Rr2nOKpc6OKNkegoB/08i/njRR89WQdZzWFErVP4sc5PH9TeTjXfrFnU2v0r+ElosWRMskQH8n4qpn5OWDoR0zU62FttqTBUt1iMTKYG2K"
            + "nFhpT+aBWu3yotyezCa52J+3Gz45tBAoIBAQD1e3U6Lp29pfBZkAU38QNLAi+/MDIFnC54DHeiaBNLci46Yl0y/At3BgamtqOEXbkqv9qnS5Phz1IXqyVF"
            + "DvoQoxhXxFHXyiT4QXQEUwiIDMAMuVXh2kfgwST4wTMgd3zOwgrK9kBHA2pna+hv927Q1V9H1lGJ3dkhAphhMQ3xwOpvA4KU/4CxEbXJ9OSf+YuyYNMdO9"
            + "paj/fr/anmz86T0ANJUT60ohBfygTdNYewMJixHAMdWtdKp8okrECaRSAqRgav5V/900r1d8LEG4tpHw+N4l8ewcJOv+6QDtgf7OWTnI4v7Hg8dgfARhKu"
            + "VDHtumdy76m2Kp9G+5brF5khAoIBAQDjMh+lS9U4h3LxlwjZFU4Z6dWppYj8F4s/B2qwVuAOSIRkcNHuIYU+iqpouYb38/j58uyllMr+yo3I/XY6gWZcgE"
            + "oBNXBN6tuM76Pha8qU5D5uPwdWeob3Gssnbh2kJceecbv/8PcbjSankY8Ypj4qxhm71DTHZFO7Qa9WT1MDBTtg5A1YiOaCVBsYLSxLegQGF3Q41UwFAunK"
            + "ILmqE/2kTZLP6TvBTaHsOLv22J2YAgOWH4e9LfEKX2NetQuYkftFIU9RRPlkaXXGgc4eR7SImsJMD8KhfQ+jSihWSOkPa+qjZG2ywk/pCSx1C1OFSEXyGj"
            + "RZyrgFy/Qmt6fqVhCHAoIBACyddo3PgR3BtfAhK8GiDQ4p5JGj6cN5QjzRT0D2F2Oj6eD0lam5gz/rmXPdR9S7z/aEDfJP2x20N2BT2580fKBfdAInjRRi"
            + "CdwQ0Uwj5y4K2zC00nYM3PltQRHw0yD4dneBbsK6hK4jYchQJVuMJdjQntOIkSM0bc0BEr6/UqB4hmMMyUPZOAN2i4qb9p1YlloiHNx4T1QcTFvYq3Nmm0"
            + "3kBWTi3jmoJr+yELY/j1ynSGkQBUTliLFp02Rc5hTjsVfdiEOZtZuFNl9sl7paozjEy2fnF5CYeH8lhO8rs34B6SutzW3KVYPvk7MPST/jz3s8YKbUBg00"
            + "q+QTv7cUf+ECggEAeqo9W8mtvW+kJ7wcEtjl6ifOLGIrq7AqhkVC3SKKpiuRD4m6To/amQHVL+W7cXRQION/0YaccyR5mOMASmZDFf5N9okbsXX0RAu+t5"
            + "6zKeBxtKRjGdXduNzGgut5JX8gX/OYRX+ca0uyaxaz4+Md/YonqrnQJTeN3bSBLmB1uVPB03ZNnleL3SH73vnEyJuAQKm5HlZLTQldoLw6ghF5CJS5h3et"
            + "w5herGOVWJlrvP6ZYRx09TcwxSDrTd8B+8YVnCV35bEP1Z7678p1tvOQDZFBBkAcHYSgRNFtJekHrEPf04gNkk5HRtKlJiyPU47J9QUg7rn80WRk1eKizm"
            + "rZUQKCAQAfpKXqkoh/fr545DRrd1EfZ95p3YqtntDI8LxmEYOwEt3Psa970kxHVWDmIrRn5sUhgKIupGB1LBzw3Bey2KGw0fCyO0wBhMQBXgIigKdYOzkq"
            + "AhUBaAMw0OFHbQVhR0poERcRLH88OnjaetFYtdhEh/uCHRfZnwi1S6EBgaL+du1tCsf1D+5spYFSEpvISnYhnagRQls5DxCg6gXd0Rfmxy4+mawyWMjAL3"
            + "uLIkkcxHTQ/dYOeFbkXLkBEKfmub2mJ/ZGiWeoyk6upeSEnYFjtRnVXz6DofEg71LfA3PcpBQ4GojOJDqtEn+sG7FQu8rjmDHW2o8lcCYim1H3ueoN\", "
            + "\"http_ca_cert\"=\"MIIFvzCCA6egAwIBAgIUe1O6tJamvnXJHOyni1Lq7MHP+acwDQYJKoZIhvcNAQELBQAwbzELMAkGA1UEBhMCQVUxEzARBgNVBAgM"
            + "ClNvbWUtU3RhdGUxITAfBgNVBAoMGEludGVybmV0IFdpZGdpdHMgUHR5IEx0ZDEoMCYGA1UEAwwfRWxhc3RpY3NlYXJjaCBUZXN0IENhc2UgSFRUUCBDQTAeF"
            + "w0yMTA5MDQxMjU5MTRaFw0yMjA5MDQxMjU5MTRaMG8xCzAJBgNVBAYTAkFVMRMwEQYDVQQIDApTb21lLVN0YXRlMSEwHwYDVQQKDBhJbnRlcm5ldCBXaWRn"
            + "aXRzIFB0eSBMdGQxKDAmBgNVBAMMH0VsYXN0aWNzZWFyY2ggVGVzdCBDYXNlIEhUVFAgQ0EwggIiMA0GCSqGSIb3DQEBAQUAA4ICDwAwggIKAoICAQDZ3Il"
            + "n8S81Y80z4DKVo8+UbB8hBwHSr17BE/YuiIFthoCne9T21Kg1h48vCqAYH8gSZHiQYUVwqbuoLbdoHvPEDmqPPAJGbnyTHum1O1aEEduALShfsFUZ5PQH8H"
            + "x4ycqLV/63iIYvb2If0pAEyzZ95qzn7SF7F2Qus1L20hmU9ZVA+ogcsOIui5OT21fNrUcKxAGM1KuZx0pK0aBG5mMLLJ5VbO/rSuetc/9EbKSnmW/LvDm1a"
            + "rBoEI9stMBA6fcLKK95JV7n5eWyCyLBjdGCWNZNn172BmxY4FS4dKMLtKoKUTaWrPgJgrvrp+G7e+9o9f3eBuP4+ja6pjTlxcMR+pzF2TWGZn+oyey3Uy15"
            + "djuuMNbZINXui/bwTEK5X43vx1QHEaTGiwbUovLHqmw5hHY6uWaLV7zCisuy7V2HWVKBIAf/78fvukNzohkxQ08FxMLDOvzIruwAQ/Yu4aGcZcKxTUDdQhE"
            + "JaYjsEQ7edf5UnTPj+gSHGihWqxHkGtwTtnat0zcVothNwn9HX2TpojPr2H3NpzeJcnlKDYGmAY8/ENxZk+vz3F0pW5jyWZOYwx2CctoZLdYEAsI2QTwLH9"
            + "3rLDRF1Fl3rp1px8XwfKkJS7Dfqjt6qaI02yiYS/YkkfouPEKR3PFEY0wXq2EzBlj1ovbWlV9PgCK0DhnQZwIDAQABo1MwUTAdBgNVHQ4EFgQUx0DyyJ1Fg"
            + "jIZDOTlY0UZjc+q9o0wHwYDVR0jBBgwFoAUx0DyyJ1FgjIZDOTlY0UZjc+q9o0wDwYDVR0TAQH/BAUwAwEB/zANBgkqhkiG9w0BAQsFAAOCAgEAOqJFIKBa"
            + "cWV30m1RVxC0GA9GOidsvN0e1WB2DM7cmff9SsdGkbz8zVNovWz67jIpLibByj+ER6gbaF/zUPrpMZQa0qGgK6qpwqRl7wJ0qDH9jf1FM//0O4yrnjBd5WG"
            + "2y5vmIlHAwhH+UVs2EiN5QYycB8jDpuV2dhhNop14/FIk24O4N0PmeDa3e5OPpsodj2vOR7B0Rx17cmHc1gLbjFgikHpq2JIlCD6Fpnr/RbUPUYxFZRsV5V"
            + "pX8a34m+EzTD9QqsWC7+mo1JMnt+SoIOlsCGAq0BH3ysh9Ha1AU8/UboY14jrNxNRRrJZxbvWyrX5nBsFxo6qZeae16cmExFQpeZqGZ2kLuihkSF5BJZb0"
            + "1A7Uq1Zr6iqGYhSEzOj89e2/v6cwiMnkowsnUdSOzzRnGDZZMOalGcxdA6zJfZJ9ydmsMPfBNt4C4s4urpkOIy2+bzcbwbOc/9KYIqiCItmbxXq81Hz86y"
            + "5R1jFUn1VsTHHUIlMT+4w4PqW7gIPoat1y1sVqRLFf8TkJ6AUq0/zlv+dBFYeaHuutrh5KnCte9u5n3+W2Rv0q9v9CXXWviHSYfjsRBUx2PFDmFjSZ6sQe"
            + "NQD0yQV1WJ79aBp3agfYfAN3ke7o+YnBFbAXy4g+2aFAtflESwkpvo3pOSz+JSBUTPgLR3f4dA/cWx187Zc=\","
            + "\"transport_cert\"=\"MIIFrzCCA5egAwIBAgIUE6nu/E57NrAOtFhYBCEZQMdFk6gwDQYJKoZIhvcNAQELBQAwZzELMAkGA1UEBhMCQVUxEzARBgNVB"
            + "AgMClNvbWUtU3RhdGUxITAfBgNVBAoMGEludGVybmV0IFdpZGdpdHMgUHR5IEx0ZDEgMB4GA1UEAwwXRWxhc3RpY3NlYXJjaCBUZXN0IENhc2UwHhcNMjE"
            + "wOTA0MTI1NzQxWhcNMjIwOTA0MTI1NzQxWjBnMQswCQYDVQQGEwJBVTETMBEGA1UECAwKU29tZS1TdGF0ZTEhMB8GA1UECgwYSW50ZXJuZXQgV2lkZ2l0c"
            + "yBQdHkgTHRkMSAwHgYDVQQDDBdFbGFzdGljc2VhcmNoIFRlc3QgQ2FzZTCCAiIwDQYJKoZIhvcNAQEBBQADggIPADCCAgoCggIBALsYS+TrqxmYJxi3VoY"
            + "hYkOEKv445UbbCmZcMsKOF8cj1xQOwgXC4dp0L8Eok8DGap19R/APM+0dZttf8oPzjn77CRvpoLIXP9eljhqZhQnmOrHnv1EQgbHIVpLrHu53rH+Ths4U4"
            + "3qaU0py83zbngvl4cRSWRQZIaD1P9Jr9lfKCA1A1UTmRl479HsaJ0VoQxEHVR5TNcnaL3CJQS3uhdWxobz1etFBF6VTe05s7gQVqpL8JMZWP2ezbk6uCHz"
            + "mqQ5LWVV3LRI9HmV+rVcMV8SdoM2JSf8Hecq9dfj4qv9CNpqYcptd+LoQsun5LHHLvSU9ecRQ2jGOUItLw29ZlNxm6OBA8T59iDZLkIraYVQLQ6buxHx/z"
            + "yXnTztEXsRo+zd6nUn/Kf6tYWhw9pyavwGzfl70rusTqY48oqmwTD7Ig3ZmlwyE9pzlusPxsDW5U4VP8tNyuvycHpmMlGao067tuM0KLofhEO8v9ETdCQD"
            + "7PwJwgWO7pKvJ0lzLxwVjs8pylx6psbiXtGmrnAKK5KQrcTd67wHoZ5INBo0ZVMZe4eWEnR9fo1/RvUzCMr2oWVSc1mf0lkVEaXs9Vdp6cLgZHHz3XFsQ+"
            + "yoZXLd7wZH5dZdqN46kveLkSWhm56d1XorSVR8cW2rSkw7j0l52xnRWAn+z7G6hlNwCM6H2mZ4nAgMBAAGjUzBRMB0GA1UdDgQWBBR7/9vDOfzvXDa6O/5"
            + "H+PWi5gXNlzAfBgNVHSMEGDAWgBR7/9vDOfzvXDa6O/5H+PWi5gXNlzAPBgNVHRMBAf8EBTADAQH/MA0GCSqGSIb3DQEBCwUAA4ICAQApx/b6qVRC37XVx"
            + "2VsCwWevqyt8urPBrYxYxNjz5s2x2GTv/EQEvF1gqJH4tpX1He7M035+ZOH0CvzFsNSL0TtfXbLXPhZv9h+HyP3mBFE9A72BCYNkvsjBrjgXTzj4b0D3kZ"
            + "jrWLRfwK1Ln5rCY6v63UK/yjnRbzz1Vak5sYuVsVgE0XvIMEPcnxRExvz9pIhCoNYMEFRjFFAyhiQr87EJIXq8TbrUm7mDihqulIh0rEJuXLcte4cXty9X"
            + "/UA8q/1kKPobeLZz6rtVzULc9bUdw+qMbwNYwRGfmTN4pG2GEBszqu3xIfkBKGdEKwg3hYJN6KDi5w7UEtBD3FwWRbPTben2Q2NjBVtMNpiJdu5iVU/inc"
            + "Np1VBN23+FcLV2T9A8C98Qu14sDq2t9o5+JdzexfssBy6uI4YN2E+Bh3bpg4RWHZsnoRaOrR/x3LtvlaY96BuJve/3A1m6AfB2yHWOAE/HOVts+wUIlIpw"
            + "m1Y2l5qCirkl9rU9YM8bPqDmQ79StOe5Vu+vtyV9UD2SbRe07e5xmKcvMLvg+vhJ7EvvZIm94I9h4wC3CA/hT322K+1MoqCIQl3LzGnyV1o0O7hEUY4Pb1"
            + "iUPLmIRLCWjUsBB/XpPrRLJhX2fhm2jzFCcyuAZQXByKbvK+MjIsZQm7MGBa0T5Izkewv56Ye1epPEQ==\", "
            + "\"transport_key\"=\"MIIJQwIBADANBgkqhkiG9w0BAQEFAASCCS0wggkpAgEAAoICAQC7GEvk66sZmCcYt1aGIWJDhCr+OOVG2wpmXDLCjhfHI9cUDs"
            + "IFwuHadC/BKJPAxmqdfUfwDzPtHWbbX/KD845++wk"
            + "b6aCyFz/XpY4amYUJ5jqx579REIGxyFaS6x7ud6x/k4bOFON6mlNKcvN8254L5eHEUlkUGSGg9T/Sa/ZXyggNQNVE5kZeO/R7GidFaEMRB1UeUzXJ2i9wi"
            + "UEt7oXVsaG89XrRQRelU3tObO4EFaqS/CTGVj9ns25Orgh85qkOS1lVdy0SPR5lfq1XDFfEnaDNiUn/B3nKvXX4+Kr/QjaamHKbXfi6ELLp+Sxxy70lPXn"
            + "EUNoxjlCLS8NvWZTcZujgQPE+fYg2S5CK2mFUC0Om7sR8f88l5087RF7EaPs3ep1J/yn+rWFocPacmr8Bs35e9K7rE6mOPKKpsEw+yIN2ZpcMhPac5brD8"
            + "bA1uVOFT/LTcrr8nB6ZjJRmqNOu7bjNCi6H4RDvL/RE3QkA+z8CcIFju6SrydJcy8cFY7PKcpceqbG4l7Rpq5wCiuSkK3E3eu8B6GeSDQaNGVTGXuHlhJ0"
            + "fX6Nf0b1MwjK9qFlUnNZn9JZFRGl7PVXaenC4GRx891xbEPsqGVy3e8GR+XWXajeOpL3i5EloZuendV6K0lUfHFtq0pMO49JedsZ0VgJ/s+xuoZTcAjOh9"
            + "pmeJwIDAQABAoICACKV/DmmQyvpD5knEyyacULP5O6379JoXYTMmGmUwNqESpcfn0hXXU732XgYmy+wvja82RaMiOnVXJVDKF6yIG5i061AQ/+IArpHlXx"
            + "fUtOgpssKbzh6F6+YvEBOjJpCrzWqPOpNvDuG2czScSZspsvGRDT5kBQCDVBm5dRtNs3FwDVK/eHNu8ZhyPEUxZu0CWnVdCu18CSPW+Ouy8jE5iK5wo9ex"
            + "cR3Bvr98rZttpY0tyKSz+2GNhRifAq5a0JDlY7Z6Pq+nCtZ9wuGHl8QHg1vojE8ptwpMp+C5JMQzPOA9v0fH1iPR5KF0b0k3c1vf1iqA5+B3sP4bfVCHS+"
            + "xXK4MNl8E8Aozpz1tDFZltvMTF9u+eJr6eBQ/IsQg1qo7qZ5QyDAGxEfIHBz3UDWjHVV506yZN0M4i2dFeya5Tq7M1QIevqtArmfid6nb3rcMuKi3mHwSh"
            + "Nyab5/1BMa1IXdElUevndNBHqdtFq9qBPDEvzs/9FO9FihWR17WQEvAa0WAh6mAS9CM/XE0wZ9uTN7mPnMpPM6f9vC2qPr84VRSfPZJGwZSDRTMqahfv+c"
            + "uNAAGAGHSzS1qIQzTOb0WkM4I5JhUSlptOUAEgmrjYko1WvXNrwCakuJbCVIx4xBZUvqb+iI0KuijTnT2ISSUGQdPMRnusWgx1iD5Nf1lix/ZAoIBAQD1y"
            + "VLbND3ku0ekNPsJuObmBJ+UhsjiiGRf3VDvpfkqJB1XHYYMnH10T/9HoZsh4NXVbblzl6eZa9lmKTkmviPmBI/Kgcr2SXMnjy249XZcFvG4N8ZdDefuMBg"
            + "p3BMUvr2q6Oe+rJkyquFboLHcMgNuyHGX9Oz+SMDztBmj8CveWp3mnvCl6aCuihBRJQTJSVzd8sG3RRzlqHHXReTzMCKY3VWhOto58F+0dUsmhZKH7nI8J"
            + "7u6NMXCiNNCVXrP6iunt49S/IItf0+YgcSbze+VwVoPpBilXi4tsuEz2yRuYCl9yf6SvqIZOKDSK4NdESzRlfPeWgFzIBk4FDiUdnxLAoIBAQDC3pzGLZs"
            + "HB91MmxJWz2rMj8B+bkWbDnLFGr6/E+MuBfMaAC54DQSK88M7sOefTjHNQ0nBq2x5sxeQP2j2edaZpAHv5HGkyVbxwpoZMK4s9XZHYxHBJ3fMP+Sfn+0Yj"
            + "BykPBnZeYvProAGnZBTbscUe3fdhfuvpbfm4ZJ3tgMnlyGa7TZR8Bjod9+4WkqnKl9ODqCKKtZ6QChPv/zzMKNFLVluwZp40cUkxecqMrIWRVnkwzcznnS"
            + "5YOEHI8hHRr3wD22iSgZp0b7I5va5jZ+QwF2LKO8xGwLV6SUQPa2j10S4VCQPXkMWjzAfQgEsT8IdG1ES4u/0fMYD7/syKMQVAoIBAQCr+72wiPOuM6XDr"
            + "xbiDLH0zdNkOJQkf0/NDK3vovGgnTiyloQQGwhl9PwqAVjt8cdu2qJj0gCCiEbNB5doFrBD6Xk8OGnuwCKF0dgqjgfOFHf7cXup7WsW7ixaThZD89v/1Y0"
            + "jjN5957hdRypta8mfIT7rF4UlwX7SiHlQj2QC6OGIWDsHvVykBRO50+9vcZg77fvC4+d+g8l02wGDcXEkCew7L1U4KYyuV0zInbqUxzLECQGBICApKVi6F"
            + "9oh1jfJ2dW+OdZVQ7pMerE6XHWDEpKUUzyzqh0h+QNAJ91sJnmh/U/XGvGOOGO/7Ja07qmv1f+Y3N4a0qES7oNQzz/VAoIBABSFBnMj2EA8RsRLS/oSK0f"
            + "RF07446F5OwKgV1edi32MKNYjEMGZdVIAax18+lbfEAVyQXEAURLble6djrrth3h0ObP+FS1p+hrJCBsA8kZPrp3Dw9nYAxhh3fwlBf1gu59bqMkqsFs1H"
            + "8wSiWEPuCzi93M/KYqMY7oPJLIwW1Ku6l36/o5QPv8zqD4sW9IQdyqsBaGm8yC6YsRLDiK5i2e8Z79u6YoxZJYDtNzPq8sGkHmzSLvJwrbGicuLrAo9W8D"
            + "MjxnYu6Ym7PUQxQgy7ot6hh8iN1WvZ3QI8dss83zeLSFP0uA/Z8cXWtTfyWnWGDWia74WYXgYL224tnXIryUCggEBAI0ABkVeJ5UD8oAXiu+jKYxy83RmI"
            + "hagEIoFdsbcXRpHYMAVCmNJAx42SqVnLX1oxeshQkbSOLfFNA313wm7KBTdFtr0ZQRtYJ9Uw2w7grwT/DK9H9jY2ImtjkPVW/SW7Qziz4rV+VA2+j5ifmB"
            + "5e/uHZGA4XY7Pe+tK1Kx/HBayR3DwQ3yy1qo/BtOl/Gdhp46+CIWASjD3XSkv7xnAtCnILJ1/p3y2SYXT7t6Ro3LxUKNnW+CwPbEhPhA0gtXeEAJeRwLgn"
            + "EkfMpth4EFR8QZcmXaPMkW5ujyL6/RmHwKmwRm/esc1/9cYzVaOu9bBO00c55m9Yvhm28tnDF+c7fs=\"}";
        final Settings settings = Settings.builder()
            .put("xpack.security.http.ssl.enabled", true)
            .put(
                "xpack.security.http.ssl.key",
                PathUtils.get(TestEnrollToCluster.class.getResource("/http.key").toURI()).toAbsolutePath().normalize()
            )
            .put(
                "xpack.security.http.ssl.certificate",
                PathUtils.get(TestEnrollToCluster.class.getResource("/http.crt").toURI()).toAbsolutePath().normalize()
            )
            .put(
                "xpack.security.http.ssl.certificate_authorities",
                PathUtils.get(TestEnrollToCluster.class.getResource("/http_ca.crt").toURI()).toAbsolutePath().normalize()
            )
            .build();
        final TestsSSLService sslService = new TestsSSLService(TestEnvironment.newEnvironment(settings));
        esNode = new MockWebServer(sslService.sslContext("xpack.security.http.ssl"), false);
        esNode.start();
        esNode.enqueue(new MockResponse().setResponseCode(200).setBody(enrollmentResponseBody));
    }

    @After
    public void shutdownMockEsNode() {
        if (esNode != null) {
            esNode.close();
        }
    }

    public void test10Install() throws Exception {
        installation = installArchive(sh, distribution());
        verifyArchiveInstallation(installation, distribution());
    }

    public void test20EnrollToClusterWithEmptyToken() throws Exception {
        Shell.Result result = Archives.runElasticsearchStartCommand(installation, sh, null, List.of("--enrollment-token"), false);
        assertThat(result.exitCode, equalTo(65));
        assertThat(esNode.requests(), empty());
    }

    public void test30EnrollToClusterWithInvalidToken() throws Exception {
        Shell.Result result = Archives.runElasticsearchStartCommand(
            installation,
            sh,
            null,
            List.of("--enrollment-token", "somerandomcharsthatarenotabase64encodedjsonstructure"),
            false
        );
        assertThat(result.exitCode, equalTo(65));
        assertThat(esNode.requests(), empty());
    }

    public void test40EnrollToClusterWithValidToken() throws Exception {
        awaitElasticsearchStartup(
            Archives.runElasticsearchStartCommand(
                installation,
                sh,
                null,
                List.of("--enrollment-token", generateMockEnrollmentToken()),
                false
            )
        );
        verifySecurityAutoConfigured(installation);
        assertThat(esNode.requests().size(), equalTo(1));
        MockRequest request = esNode.takeRequest();
        assertThat(request.getHeader("Authorization"), equalTo("ApiKey c29tZS1hcGkta2V5")); // base64(some-api-key)
        assertThat(
            request.getUri().toString(),
            equalTo("https://" + esNode.getHostName() + ":" + esNode.getPort() + "/_security/enroll/node")
        );
    }

    public void test50EnrollmentFailsForConfiguredNode() throws Exception {
        stopElasticsearch();
        Shell.Result result = Archives.runElasticsearchStartCommand(
            installation,
            sh,
            null,
            List.of("--enrollment-token", generateMockEnrollmentToken()),
            false
        );
        assertThat(result.exitCode, equalTo(78));
        assertThat(esNode.requests(), empty());
    }

    public void test60MultipleValuesForEnrollmentToken() throws Exception {
        // if invoked with --enrollment-token tokenA tokenB tokenC, only tokenA is read
        Shell.Result result = Archives.runElasticsearchStartCommand(
            installation,
            sh,
            null,
            List.of("--enrollment-token", generateMockEnrollmentToken(), "some-other-token", "some-other-token", "some-other-token"),
            false
        );
        // Assert we used the first value which is a proper enrollment token but failed because the node is already configured ( 78 )
        assertThat(result.exitCode, equalTo(78));
        assertThat(esNode.requests(), empty());
    }

    public void test70MultipleParametersForEnrollmentToken() throws Exception {
        // if invoked with --enrollment-token tokenA --enrollment-token tokenB --enrollment-token tokenC, only tokenC is used
        Shell.Result result = Archives.runElasticsearchStartCommand(
            installation,
            sh,
            null,
            List.of(
                "--enrollment-token",
                "some-other-token",
                "--enrollment-token",
                "some-other-token",
                "--enrollment-token",
                generateMockEnrollmentToken()
            ),
            false
        );
        // Assert we used the last named parameter which is a proper enrollment token but failed because the node is already configured (78)
        assertThat(result.exitCode, equalTo(78));
        assertThat(esNode.requests(), empty());
    }

    private String generateMockEnrollmentToken() throws Exception {
        EnrollmentToken enrollmentToken = new EnrollmentToken(
            "some-api-key",
            "E8:86:4F:A9:CB:5A:80:53:EA:84:A4:85:81:A6:C9:BE:F6:19:F8:F6:AA:A5:8A:63:2A:AC:3E:0A:25:D4:3E:A9",
            Version.CURRENT.toString(),
            List.of(esNode.getHostName() + ":" + esNode.getPort())
        );
        return enrollmentToken.getEncoded();
    }

    private static void verifySecurityAutoConfigured(Installation es) throws Exception {
        Optional<String> autoConfigDirName = getAutoConfigPathDir(es);
        assertThat(autoConfigDirName.isPresent(), is(true));
        assertThat(es.config(autoConfigDirName.get()), file(Directory, "root", "elasticsearch", p750));
        Stream.of("http_keystore_local_node.p12", "http_ca.crt", "transport_keystore_all_nodes.p12")
            .forEach(file -> assertThat(es.config(autoConfigDirName.get()).resolve(file), file(File, "root", "elasticsearch", p660)));
        assertThat(
            PemUtils.readCertificates(List.of(es.config(autoConfigDirName.get()).resolve("http_ca.crt"))).get(0),
            equalTo(
                PemUtils.readCertificates(
                    List.of(PathUtils.get(TestEnrollToCluster.class.getResource("/http_ca.crt").toURI()).toAbsolutePath().normalize())
                ).get(0)
            )
        );
        List<String> configLines = Files.readAllLines(es.config("elasticsearch.yml"));

        assertThat(configLines, hasItem("xpack.security.enabled: true"));
        assertThat(configLines, hasItem("xpack.security.http.ssl.enabled: true"));
        assertThat(configLines, hasItem("xpack.security.transport.ssl.enabled: true"));
        assertThat(configLines, hasItem("xpack.security.enabled: false"));
        assertThat(configLines, hasItem("xpack.security.http.ssl.enabled: false"));
        assertThat(configLines, hasItem("xpack.security.transport.ssl.enabled: false"));

        assertThat(configLines, hasItem("xpack.security.enrollment.enabled: true"));
        assertThat(configLines, hasItem("xpack.security.transport.ssl.verification_mode: certificate"));
        assertThat(
            configLines,
            hasItem(
                "xpack.security.transport.ssl.keystore.path: "
                    + es.config(autoConfigDirName.get()).resolve("transport_keystore_all_nodes.p12")
            )
        );
        assertThat(
            configLines,
            hasItem(
                "xpack.security.transport.ssl.truststore.path: "
                    + es.config(autoConfigDirName.get()).resolve("transport_keystore_all_nodes.p12")
            )
        );
        assertThat(
            configLines,
            hasItem("xpack.security.http.ssl.keystore.path: " + es.config(autoConfigDirName.get()).resolve("http_keystore_local_node.p12"))
        );
        assertThat(configLines, hasItem("http.host: [_local_, _site_]"));
    }

    private static Optional<String> getAutoConfigPathDir(Installation es) {
        final Shell.Result lsResult = sh.run("find \"" + es.config + "\" -type d -maxdepth 1");
        assertNotNull(lsResult.stdout);
        return Arrays.stream(lsResult.stdout.split("\n")).filter(f -> f.contains("tls_auto_config_node_")).findFirst();
    }
}
