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
        DataSet data = new DataSet();
        data.intializeSpark_ml();
        data.showData(10);
        data.clean();
    }
    
}
