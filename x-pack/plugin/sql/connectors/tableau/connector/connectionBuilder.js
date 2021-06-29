(function dsbuilder(attr) {
    var urlBuilder = "jdbc:es://";

    if (attr["sslmode"] == "require") {
        urlBuilder += "https://";
    } else{
        urlBuilder += "http://";
    }
    urlBuilder += attr["server"] + ":" + attr["port"] + "?";

    return [urlBuilder];
})
