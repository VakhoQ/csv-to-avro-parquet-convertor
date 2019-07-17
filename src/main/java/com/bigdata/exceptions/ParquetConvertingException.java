package com.bigdata.exceptions;

public class ParquetConvertingException extends  Exception {
    public ParquetConvertingException() {
    }

    public ParquetConvertingException(String s) {
        super(s);
    }

    public ParquetConvertingException(String s, Throwable throwable) {
        super(s, throwable);
    }

    public ParquetConvertingException(Throwable throwable) {
        super(throwable);
    }

    public ParquetConvertingException(String s, Throwable throwable, boolean b, boolean b1) {
        super(s, throwable, b, b1);
    }
}
