package com.example.data.resultutil;

import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * @author zx
 */
@NoArgsConstructor
@Data
public class ResultData {
    private Integer code;

    private String msg;


    private Object data;

    public ResultData(Integer code, String msg) {
        this.code = code;
        this.msg = msg;
    }
    public ResultData(Integer code, Object data) {
        this.code = code;
        this.data = data;
    }
    public ResultData(Integer code,String msg, Object data) {
        this.code = code;
        this.msg = msg;
        this.data = data;
    }
    public ResultData(Integer code) {
        this.code = code;
    }

    public static ResultData error404(String msg) {
        return new ResultData(404, msg);
    }


    public static ResultData error500(String msg) {
        return new ResultData(500, msg);
    }


    public static ResultData okRow(List row) {
        return new ResultData(200, row);
    }
    public static ResultData okData(Object data) {
        return new ResultData(200, data);
    }
    public static ResultData ok() {
        return new ResultData(200);
    }

    public static ResultData ok(String msg) {
        return new ResultData(200, msg);
    }
    public static ResultData ok(String msg,Object data) {
        return new ResultData(200, msg,data);
    }
}
