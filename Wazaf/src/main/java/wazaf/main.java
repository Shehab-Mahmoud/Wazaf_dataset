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
        System.out.println(data.getStructure());
        System.out.println(data.getSummary());
        System.out.println(data.plotCompanyPieChart(10));
        System.out.println(data.getMostDemandingCompanies(10));
        System.out.println(data.PlotTitleForCompany(10));
        System.out.println(data.plotAreaBarChart(10));
        System.out.println(data.getMostDemandedSkills());
        System.out.println(data.getFactorizedYearsOfExp(10));
        System.out.println(data.kMeansAlgorithm());

//        data.getStructure();
//        data.getSummary();
//        data.plotCompanyPieChart(10);


    }

}
