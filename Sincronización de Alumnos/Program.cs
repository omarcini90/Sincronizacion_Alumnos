using MySql.Data.MySqlClient;
using Oracle.DataAccess.Client;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Text;


namespace Sincronización_de_Alumnos
{

    static class Globales
    {
        public static string strFileName;
    }

    class Program
    {
        static void Main(string[] args)
        {
            Sincroniza_Alumnos();
        }
        static void RegistrarSQLServ(DataTable TablaUsuarios)
        {
            MySqlConnection MySqlCnn = new MySqlConnection(System.Configuration.ConfigurationManager.ConnectionStrings["MysqlConnectionString"].ConnectionString);
            MySqlCommand MySqlCmd = new MySqlCommand();
            int Resultado = 0;

            string strMatricula = "";

            int i;
            if (MySqlCnn.State == ConnectionState.Closed)
                MySqlCnn.Open();
            MySqlCmd.Connection = MySqlCnn;
            MySqlCmd.CommandType = CommandType.StoredProcedure;
            MySqlCmd.CommandText = "Inserta_AlumnoNuevo";
            if (TablaUsuarios.Rows.Count > 0)
            {
                for (i = 0; i < TablaUsuarios.Rows.Count; i++)
                {
                    
                    strMatricula = TablaUsuarios.Rows[i]["Matricula"].ToString();
                    MySqlCmd.Parameters.AddWithValue("@IDAlumno_in", strMatricula);
                    MySqlCmd.Parameters.AddWithValue("@Nombre_in", TablaUsuarios.Rows[i]["Nombre"].ToString());
                    MySqlCmd.Parameters.AddWithValue("@Email_in", TablaUsuarios.Rows[i]["Email"].ToString());
                    MySqlCmd.Parameters.AddWithValue("@Campus_in", TablaUsuarios.Rows[i]["Campus_code"].ToString());
                    MySqlCmd.Parameters.AddWithValue("@Programa_in", TablaUsuarios.Rows[i]["Programa_desc"].ToString());
                    MySqlCmd.Parameters.AddWithValue("@CampDesc_in", TablaUsuarios.Rows[i]["Campus_desc"].ToString());
                    MySqlCmd.Parameters.AddWithValue("@IDModalidad_in", TablaUsuarios.Rows[i]["Modalidad"].ToString());
                    MySqlCmd.Parameters.AddWithValue("@IDNivel_in", TablaUsuarios.Rows[i]["Nivel_code"].ToString());
                    MySqlCmd.Parameters.AddWithValue("@CodigoProcedencia_in", TablaUsuarios.Rows[i]["ClaveProcedencia"].ToString());
                    MySqlCmd.Parameters.AddWithValue("@CodigoTipoIngreso_in", TablaUsuarios.Rows[i]["TipoIngreso"].ToString());
                    MySqlCmd.Parameters.AddWithValue("@IDEstatus_saes_in", TablaUsuarios.Rows[i]["Estatus_code"].ToString());
                    MySqlCmd.Parameters.AddWithValue("@Estatus_saes_in", TablaUsuarios.Rows[i]["Estatus_desc"].ToString());
                    MySqlCmd.Parameters.AddWithValue("@f_inicio", TablaUsuarios.Rows[i]["FechaInicio"].ToString());
                    MySqlCmd.Parameters.AddWithValue("@Periodo_admision_in", TablaUsuarios.Rows[i]["PeriodoAdmision"].ToString());
                    MySqlCmd.Parameters.AddWithValue("@Cod_Program", TablaUsuarios.Rows[i]["Programa_code"].ToString());
                    MySqlCmd.Parameters.Add("@Result", MySqlDbType.Int32);
                    MySqlCmd.Parameters["@Result"].Direction = ParameterDirection.Output;
                    try
                    {
                        Console.WriteLine("Insertando registro: " + strMatricula);
                        MySqlCmd.ExecuteNonQuery();
                        Resultado = (Int32)MySqlCmd.Parameters["@Result"].Value;
                        MySqlCmd.Parameters.Clear();


                        if (Resultado == 1)
                        {
                            Console.WriteLine("Se inserto el registro : " + strMatricula);
                        }
                        else
                        {
                            Console.WriteLine("Registro Existente :" + strMatricula);
                        }


                    }
                    catch (MySqlException ex)
                    {
                        Console.WriteLine("Error Insertando Registro: " + strMatricula);
                        MySqlCmd.Parameters.Clear();
                        RegistraErrores("Matricula:" + TablaUsuarios.Rows[i]["Matricula"].ToString() + " - " + ex.ToString());
                    }
                }
            }
            else
            {
                RegistraErrores("La consulta no arrojó ningún registro");
            }
            MySqlCmd = null;
        }

        static void RegistraErrores(string MensajeError)
        {
            StreamWriter FileLog;
            string RutaArchivo = String.Empty;
            string strLogError = "Error Ocurrido el: " + DateTime.Today.Date.ToShortDateString().ToString() + " a las " + string.Format("{0:HH:mm:ss tt}", DateTime.Now);

            RutaArchivo = System.Configuration.ConfigurationManager.AppSettings["strFileName"].ToString() + Globales.strFileName + ".log";

            if (!File.Exists(RutaArchivo))
            {
                FileLog = new StreamWriter(RutaArchivo);
            }
            else
            {
                FileLog = File.AppendText(RutaArchivo);
            }
            FileLog.WriteLine("/*******************************************************************************\\");
            FileLog.WriteLine(strLogError);
            FileLog.WriteLine(MensajeError);
            FileLog.WriteLine();
            FileLog.Close();
        }

        static void Sincroniza_Alumnos()
        {

            try
            {
                
                string strQuery = "";
                strQuery = "SELECT TPERS_ID MATRICULA, CONCAT(TPERS_NOMBRE,' ',TPERS_PATERNO,' ',TPERS_MATERNO) NOMBRE , " +
                            "(SELECT TALCO_CORREO FROM TALCO " +
                              "WHERE TALCO_TPERS_NUM=TPERS_NUM AND TALCO_ESTATUS='A') EMAIL, " +
                              "TESTU_TCAMP_CLAVE CAMPUS_CODE, TCAMP_DESC CAMPUS_DESC, TESTU_TPROG_CLAVE PROGRAMA_CODE, " +
                              "TPROG_DESC PROGRAMA_DESC, TPROG_TNIVE_CLAVE NIVEL_CODE, TNIVE_DESC NIVEL_DESC,TPROG_TMODA_CLAVE MODALIDAD, " +
                              "'N' CLAVEPROCEDENCIA, " +
                            "(SELECT TADMI_TTIIN_CLAVE FROM TADMI Z, TPROG TT " +
                             "WHERE TADMI_TPERS_NUM=TPERS_NUM AND TADMI_TPEES_CLAVE IN (SELECT MIN(TADMI_TPEES_CLAVE) FROM TADMI ZZ " +
                                 "WHERE Z.TADMI_TPERS_NUM=ZZ.TADMI_TPERS_NUM AND ZZ.TADMI_TCAMP_CLAVE=TESTU_TCAMP_CLAVE " +
                                 "AND   ZZ.TADMI_TPROG_CLAVE=TT.TPROG_CLAVE AND T.TPROG_TNIVE_CLAVE=TT.TPROG_TNIVE_CLAVE)) TIPOINGRESO, " +
                            "TESTU_TSTAL_CLAVE ESTATUS_CODE, TSTAL_DESC ESTATUS_DESC, " +
                            "(SELECT TADMI_TPEES_CLAVE FROM TADMI Z, TPROG TT " +
                             "WHERE TADMI_TPERS_NUM=TPERS_NUM AND TADMI_TPEES_CLAVE IN (SELECT MIN(TADMI_TPEES_CLAVE) FROM TADMI ZZ " +
                                 "WHERE Z.TADMI_TPERS_NUM=ZZ.TADMI_TPERS_NUM AND ZZ.TADMI_TCAMP_CLAVE=TESTU_TCAMP_CLAVE " +
                                 "AND   ZZ.TADMI_TPROG_CLAVE=TT.TPROG_CLAVE AND T.TPROG_TNIVE_CLAVE=TT.TPROG_TNIVE_CLAVE)) PERIODOADMISION, " +
                            "(SELECT DATE_FORMAT(TPEES_INICIO,'%d/%m/%Y') FROM TADMI Z, TPROG TT, TPEES " +
                             "WHERE TADMI_TPERS_NUM=TPERS_NUM AND TADMI_TPEES_CLAVE IN (SELECT MIN(TADMI_TPEES_CLAVE) FROM TADMI ZZ " +
                                 "WHERE Z.TADMI_TPERS_NUM=ZZ.TADMI_TPERS_NUM AND ZZ.TADMI_TCAMP_CLAVE=TESTU_TCAMP_CLAVE " +
                                 "AND   ZZ.TADMI_TPROG_CLAVE=TT.TPROG_CLAVE AND T.TPROG_TNIVE_CLAVE=TT.TPROG_TNIVE_CLAVE " +
                                 "AND    ZZ.TADMI_TPEES_CLAVE=TPEES_CLAVE)) FECHAINICIO " +
                            "FROM TPERS, TESTU S, TCAMP, TPROG T, TNIVE, TSTAL " +
                            "WHERE TPERS_TIPO='E' " +
                            "AND   TPERS_NUM=TESTU_TPERS_NUM AND TESTU_TPEES_CLAVE IN ( SELECT MAX(TESTU_TPEES_CLAVE) " +
                                 "FROM TESTU SS WHERE S.TESTU_TPERS_NUM=SS.TESTU_TPERS_NUM) " +
                            "AND   TESTU_TCAMP_CLAVE=TCAMP_CLAVE AND TESTU_TPROG_CLAVE=TPROG_CLAVE AND TPROG_TNIVE_CLAVE=TNIVE_CLAVE " +
                            "AND   TESTU_TSTAL_CLAVE=TSTAL_CLAVE";

                MySqlConnection mysql_con = new MySqlConnection(ConfigurationManager.ConnectionStrings["MysqlConnectionStringSAES"].ConnectionString);
                mysql_con.Open();
                MySqlCommand mysqlcmd = new MySqlCommand(strQuery, mysql_con);
                MySqlDataAdapter mysql_da = new MySqlDataAdapter(strQuery, mysql_con);
                DataTable dt = new DataTable();
                mysql_da.Fill(dt);
                mysql_con.Close();

                RegistrarSQLServ(dt);


            }
            catch (Exception ex)
            {
                RegistraErrores(ex.ToString());
            }

        }

    }
}
