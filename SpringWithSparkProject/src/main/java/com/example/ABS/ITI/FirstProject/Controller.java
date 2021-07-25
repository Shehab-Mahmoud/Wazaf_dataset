package com.example.ABS.ITI.FirstProject;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;
import wazaf.DataSet;

import java.util.List;

@RestController
public class Controller {

//    @Autowired
//    private Dataset<Row> mDataset;
    DataSet mydata = new DataSet();
    @RequestMapping(value = "/"  , produces     =  MediaType.TEXT_PLAIN_VALUE)
    public String getTest(){
        return mydata.getSummary();

    }
//
//    //Load Dataset
//    @GetMapping(value = "/loadDataset" , produces     =  MediaType.TEXT_PLAIN_VALUE )
//    public @ResponseBody
//    String show()
//    {return mDataset.showString(10,0,false); }




}
