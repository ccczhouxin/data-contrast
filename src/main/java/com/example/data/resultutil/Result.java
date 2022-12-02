package com.example.data.resultutil;

import lombok.Data;

import java.util.List;

/**
 * 接口返回结果
 *
 * @author zx
 */
@Data
public class Result {

    ResultData resultData;

    public Result() {
        resultData = new ResultData();
    }

    public Result(Integer code, String msg) {
        this(code);
        resultData.setMsg(msg);
    }

    public Result(Integer code, Object data) {
        this(code);
        resultData.setData(data);
    }
    public Result(Integer code, String msg, Object data) {
        this(code);
        resultData.setMsg(msg);
        resultData.setData(data);
    }
    public Result(Integer code) {
        this();
        resultData.setCode(code);
    }

    public static Result error404(String msg) {
        return new Result(404, msg);
    }


    public static Result error500(String msg) {
        return new Result(1, msg);
    }

    public static Result error500(String msg,Object data) {
        return new Result(1, msg,data);
    }
    public static Result ok(String msg,List row) {
        return new Result(200, msg,row);
    }

    public static Result okRow(List row) {
        return new Result(200, row);
    }

    public static Result okData(Object data) {
        return new Result(200, data);
    }

    public static Result ok() {
        return new Result(200);
    }
    public static Result ok(String msg) {
        return new Result(200, msg);
    }
    public static Result ok(String msg,Object data) {
        return new Result(200, msg,data);
    }
}
