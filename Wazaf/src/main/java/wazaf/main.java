/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package wazaf;

import java.io.IOException;

/**
 *
 * @author shehab
 */
public class main {
    public static void main(String[] args) throws IOException{
        System.out.println("here");
        DataSet data = new DataSet();
        data.clean();
        System.out.println(data.head(10));
        data.head(10);
//        data.getStructure();
//        data.getSummary();
//        data.plotCompanyPieChart(10);


    }

}
