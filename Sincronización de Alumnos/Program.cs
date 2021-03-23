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
            Sincroniza_Online();
            Sincroniza_WA();
        }
        static void RegistrarSQLServ(DataTable TablaUsuarios)
        {
            SqlConnection sqlCnn = new SqlConnection(System.Configuration.ConfigurationManager.ConnectionStrings["sqlConnectionString"].ConnectionString);
            SqlCommand sqlCmd = new SqlCommand();
            int Resultado = 0;

            string strCorreo = "";
            string strMatricula = "";

            int i;
            if (sqlCnn.State == ConnectionState.Closed)
                sqlCnn.Open();
            sqlCmd.Connection = sqlCnn;
            sqlCmd.CommandType = CommandType.StoredProcedure;
            sqlCmd.CommandText = "Inserta_AlumnoNuevo";
            if (TablaUsuarios.Rows.Count > 0)
            {
                for (i = 0; i < TablaUsuarios.Rows.Count; i++)
                {
                    strCorreo = "";

                    if (TablaUsuarios.Rows[i]["Email1"].ToString() != "")
                        strCorreo = TablaUsuarios.Rows[i]["Email1"].ToString();
                    else
                        if (TablaUsuarios.Rows[i]["Email2"].ToString() != "")
                        strCorreo = TablaUsuarios.Rows[i]["Email2"].ToString();
                    else
                            if (TablaUsuarios.Rows[i]["Email3"].ToString() != "")
                        strCorreo = TablaUsuarios.Rows[i]["Email3"].ToString();
                    else
                                if (TablaUsuarios.Rows[i]["Email4"].ToString() != "")
                        strCorreo = TablaUsuarios.Rows[i]["Email4"].ToString();


                    strMatricula = TablaUsuarios.Rows[i]["Matricula"].ToString();
                    sqlCmd.Parameters.AddWithValue("@IDAlumno", strMatricula);
                    sqlCmd.Parameters.AddWithValue("@Nombre", TablaUsuarios.Rows[i]["Nombre"].ToString());
                    sqlCmd.Parameters.AddWithValue("@Email", strCorreo);
                    sqlCmd.Parameters.AddWithValue("@Campus", TablaUsuarios.Rows[i]["Campus"].ToString());
                    sqlCmd.Parameters.AddWithValue("@Programa", TablaUsuarios.Rows[i]["Programa"].ToString());
                    sqlCmd.Parameters.AddWithValue("@CampDesc", TablaUsuarios.Rows[i]["CampDesc"].ToString());
                    sqlCmd.Parameters.AddWithValue("@IDArea", TablaUsuarios.Rows[i]["Codigo_Area"].ToString());
                    sqlCmd.Parameters.AddWithValue("@IDNivel", TablaUsuarios.Rows[i]["Nivel"].ToString());
                    sqlCmd.Parameters.AddWithValue("@CodigoProcedencia", TablaUsuarios.Rows[i]["CodigoProcedencia"].ToString());
                    sqlCmd.Parameters.AddWithValue("@CodigoTipoIngreso", TablaUsuarios.Rows[i]["CodigoTipoIngreso"].ToString());
                    sqlCmd.Parameters.AddWithValue("@fecha_insert", DateTime.Now);
                    sqlCmd.Parameters.AddWithValue("@IDEstatus_Banner", TablaUsuarios.Rows[i]["Cod_Estatus_Banner"].ToString());
                    sqlCmd.Parameters.AddWithValue("@Estatus_Banner", TablaUsuarios.Rows[i]["Estatus_Banner"].ToString());
                    sqlCmd.Parameters.AddWithValue("@fmateria_1", TablaUsuarios.Rows[i]["FECHA_1_MATERIA"].ToString());
                    sqlCmd.Parameters.AddWithValue("@fmateria_2", TablaUsuarios.Rows[i]["FECHA_2_MATERIA"].ToString());
                    sqlCmd.Parameters.AddWithValue("@fmateria_3", TablaUsuarios.Rows[i]["FECHA_3_MATERIA"].ToString());
                    sqlCmd.Parameters.AddWithValue("@Periodo_admision", TablaUsuarios.Rows[i]["PERIODO_ADMISION"].ToString());
                    sqlCmd.Parameters.AddWithValue("@Cod_Program", TablaUsuarios.Rows[i]["COD_PROGRAM"].ToString());
                    try
                    {
                        Console.WriteLine("Insertando registro: " + strMatricula);

                        Resultado = Convert.ToInt32(sqlCmd.ExecuteScalar());
                        sqlCmd.Parameters.Clear();


                        if (Resultado == 1)
                        {
                            Console.WriteLine("Se inserto el registro : " + strMatricula);
                        }
                        else
                        {
                            Console.WriteLine("Registro Existente :" + strMatricula);
                        }


                    }
                    catch (SqlException ex)
                    {
                        Console.WriteLine("Error Insertando Registro: " + strMatricula);
                        sqlCmd.Parameters.Clear();
                        RegistraErrores("Matricula:" + TablaUsuarios.Rows[i]["Matricula"].ToString() + " - " + ex.ToString());
                    }
                }
            }
            else
            {
                RegistraErrores("La consulta no arrojó ningún registro");
            }
            sqlCmd = null;
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

        static void Sincroniza_Online()
        {
            //OracleConnection oraCnn = new OracleConnection(ConfigurationManager.ConnectionStrings["ConexionNOAH"].ConnectionString);
            //OracleConnection oraCnn = new OracleConnection(ConfigurationManager.ConnectionStrings["ConexionNOAH_UAT"].ConnectionString);
            OracleDataReader oraDR;
            DataTable objDT = new DataTable();

            try
            {
                string Periodo = "";
                string Nivel = "";
                string Matricula = "";
                if (ConfigurationManager.AppSettings["PeriodoInicio"].ToString() != "")
                {
                    Periodo = "AND SARADAP_TERM_CODE_ENTRY >= (" + ConfigurationManager.AppSettings["PeriodoInicio"].ToString() + ") ";
                }
                if (ConfigurationManager.AppSettings["Niveles"].ToString() != "")
                {
                    Nivel = "AND SGBSTDN_LEVL_CODE IN (" + ConfigurationManager.AppSettings["Niveles"].ToString() + ") ";
                }
                if (ConfigurationManager.AppSettings["Matricula"].ToString() != "")
                {
                    Matricula = "AND SPRIDEN_ID IN (" + ConfigurationManager.AppSettings["Matricula"].ToString() + ")";
                }

                string strQuery = "";
                strQuery = "SELECT DISTINCT MATRICULA,NOMBRE,EMAIL1,EMAIL2,EMAIL3,EMAIL4,CAMPUS,PROGRAMA,CAMPDESC,CODIGO_AREA,NIVEL,CODIGOPROCEDENCIA,CODIGOTIPOINGRESO,COD_ESTATUS_BANNER,ESTATUS_BANNER,FECHA_1_MATERIA,FECHA_2_MATERIA,FECHA_3_MATERIA,PERIODO_ADMISION,COD_PROGRAM " +
"FROM ( " +
"SELECT DISTINCT MATRICULA,NOMBRE,EMAIL1,EMAIL2,EMAIL3,EMAIL4,CAMPUS,PROGRAMA,CAMPDESC,CODIGO_AREA,NIVEL,CODIGOPROCEDENCIA,CODIGOTIPOINGRESO,COD_ESTATUS_BANNER,ESTATUS_BANNER, " +
"MAX(CASE WHEN IDMATERIA=1 THEN SFRSTCR_RSTS_DATE ELSE NULL END) FECHA_1_MATERIA, " +
"MAX(CASE WHEN IDMATERIA=2 THEN SFRSTCR_RSTS_DATE ELSE NULL END) FECHA_2_MATERIA, " +
"MAX(CASE WHEN IDMATERIA=3 THEN SFRSTCR_RSTS_DATE ELSE NULL END) FECHA_3_MATERIA,MAX(SARADAP_TERM_CODE_ENTRY)PERIODO_ADMISION,SMRPRLE_PROGRAM COD_PROGRAM,SPRIDEN_PIDM " +
"FROM ( " +
"SELECT DISTINCT MATRICULA,NOMBRE,EMAIL1,EMAIL2,EMAIL3,EMAIL4,CAMPUS,PROGRAMA,CAMPDESC,CODIGO_AREA,NIVEL,CODIGOPROCEDENCIA,CODIGOTIPOINGRESO,COD_ESTATUS_BANNER,ESTATUS_BANNER,MAX_SGBSTDN_TERM_CODE_EFF,MATERIA,SFRSTCR_TERM_CODE,SFRSTCR_RSTS_DATE,ROW_NUMBER() OVER(PARTITION BY MATRICULA ORDER BY SFRSTCR_RSTS_DATE) IDMateria,SARADAP_TERM_CODE_ENTRY,SMRPRLE_PROGRAM,SPRIDEN_PIDM " +
"FROM( " +
 "SELECT DISTINCT SPRIDEN_ID MATRICULA, SPRIDEN_FIRST_NAME || ' ' || SPRIDEN_LAST_NAME NOMBRE, " +
                  "NVL(P1.GOREMAL_EMAIL_ADDRESS,'') EMAIL1, " +
                  "NVL(P2.GOREMAL_EMAIL_ADDRESS,'') EMAIL2, " +
                  "NVL(E1.GOREMAL_EMAIL_ADDRESS,'') EMAIL3, " +
                  "NVL(U1.GOREMAL_EMAIL_ADDRESS,'') EMAIL4, " +
                  "TO_NUMBER(SORLCUR_CAMP_CODE) Campus, SMRPRLE_PROGRAM_DESC PROGRAMA, STVCAMP_DESC CampDesc, " +
                  "CASE SMRPRLE_DEGC_CODE  WHEN  'BACHIL' THEN 1 WHEN 'LICENC' THEN 2 WHEN 'MAESTR' THEN 3  ELSE 4 END Codigo_Area, SGBSTDN_LEVL_CODE Nivel, " +
                  "SARADAP_RESD_CODE CodigoProcedencia, SARADAP_STYP_CODE CodigoTipoIngreso,SGBSTDN_STST_CODE Cod_Estatus_Banner,STVSTST_DESC Estatus_Banner,MAX_SGBSTDN_TERM_CODE_EFF,SSBSECT_SUBJ_CODE||SCBCRSE_CRSE_NUMB MATERIA,SFRSTCR_TERM_CODE,SFRSTCR_RSTS_DATE,SARADAP_TERM_CODE_ENTRY,SMRPRLE_PROGRAM,SPRIDEN_PIDM " +
                  "FROM SGBSTDN " +
                  "JOIN (  SELECT  SGBSTDN_PIDM MAX_SGBSTDN_PIDM, " +
                                 "MAX(SGBSTDN_TERM_CODE_EFF) MAX_SGBSTDN_TERM_CODE_EFF " +
                          "FROM SGBSTDN " +
                          "GROUP BY SGBSTDN_PIDM) SGBSTDN_MAX ON MAX_SGBSTDN_PIDM = SGBSTDN_PIDM AND MAX_SGBSTDN_TERM_CODE_EFF = SGBSTDN_TERM_CODE_EFF " +
                  "JOIN (SELECT DISTINCT SORLCUR_PIDM, SORLCUR_PROGRAM, SORLCUR_CAMP_CODE " +
                               "FROM SORLCUR " +
                               "WHERE SORLCUR_CACT_CODE='ACTIVE' " +
                               "AND SORLCUR_TERM_CODE=(SELECT DISTINCT MAX(X.SORLCUR_TERM_CODE) FROM SORLCUR X WHERE X.SORLCUR_PIDM=SORLCUR.SORLCUR_PIDM AND " +
                  "X.SORLCUR_CACT_CODE='ACTIVE')) ON SORLCUR_PIDM=SGBSTDN_PIDM AND SORLCUR_PROGRAM=SGBSTDN_PROGRAM_1 " +
                  "JOIN SPRIDEN ON SPRIDEN_PIDM = SGBSTDN_PIDM " +
                  "JOIN SMRPRLE ON SMRPRLE_PROGRAM = SGBSTDN_PROGRAM_1 " +
                  "JOIN STVCAMP ON STVCAMP_CODE = SORLCUR_CAMP_CODE " +
                  "JOIN SARADAP ON SARADAP_PIDM = SPRIDEN_PIDM " +
                  "JOIN SARAPPD ON SARAPPD_PIDM = SGBSTDN_PIDM AND SARAPPD_APDC_CODE IN('CA','AC','PP') " +
                  "JOIN STVSTST ON STVSTST_CODE=SGBSTDN_STST_CODE " +
                  "LEFT JOIN SFRSTCR ON SFRSTCR_PIDM=SPRIDEN_PIDM AND SFRSTCR_TERM_CODE >= MAX_SGBSTDN_TERM_CODE_EFF AND SFRSTCR_RSTS_CODE='RE' " +
                  "LEFT JOIN SSBSECT ON SSBSECT_CRN= SFRSTCR_CRN AND SSBSECT_TERM_CODE=SFRSTCR_TERM_CODE " +
                  "LEFT JOIN (SELECT DISTINCT SMRPAAP_PROGRAM,SMRACAA_AREA,SCBCRSE_CRSE_NUMB,SCBCRSE_SUBJ_CODE,SMRACAA_SEQNO,SMRPAAP_AREA_PRIORITY " +
                                    "FROM " +
                                    "SMRPAAP " +
                                    "JOIN SMRALIB ON SMRALIB_AREA = SMRPAAP_AREA " +
                                    "JOIN SMRACAA ON SMRACAA_AREA = SMRPAAP_AREA " +
                                    "JOIN SCBCRSE ON SCBCRSE_SUBJ_CODE = SMRACAA_SUBJ_CODE AND SCBCRSE_CRSE_NUMB = SMRACAA_CRSE_NUMB_LOW " +
                                    "JOIN SCRSYLN ON SCRSYLN_SUBJ_CODE = SCBCRSE_SUBJ_CODE AND SCRSYLN_CRSE_NUMB = SCBCRSE_CRSE_NUMB)ON SMRPAAP_PROGRAM=SGBSTDN_PROGRAM_1 AND SSBSECT_SUBJ_CODE=SCBCRSE_SUBJ_CODE AND SSBSECT_CRSE_NUMB=SCBCRSE_CRSE_NUMB " +
                  "LEFT JOIN GOREMAL P1 ON P1.GOREMAL_PIDM = SPRIDEN_PIDM AND P1.GOREMAL_EMAL_CODE = 'PER1' " +
                  "LEFT JOIN GOREMAL P2 ON P2.GOREMAL_PIDM = SPRIDEN_PIDM AND P2.GOREMAL_EMAL_CODE = 'PER2' " +
                  "LEFT JOIN GOREMAL E1 ON E1.GOREMAL_PIDM = SPRIDEN_PIDM AND E1.GOREMAL_EMAL_CODE = 'EMPR' " +
                  "LEFT JOIN GOREMAL U1 ON U1.GOREMAL_PIDM = SPRIDEN_PIDM AND U1.GOREMAL_EMAL_CODE = 'STAN' AND U1.GOREMAL_PREFERRED_IND='Y' " +
                  "WHERE 1 = 1 " +
                  "AND SPRIDEN_CHANGE_IND IS NULL " +
                  "AND SPRIDEN_ID LIKE 'U%' " +
                  "AND SARADAP_STYP_CODE NOT IN ('0','T','E') " +
                  Periodo +
                  Nivel +
                  Matricula +
                  ")) " +
                  "GROUP BY MATRICULA,NOMBRE,EMAIL1,EMAIL2,EMAIL3,EMAIL4,CAMPUS,PROGRAMA,CAMPDESC,CODIGO_AREA,NIVEL,CODIGOPROCEDENCIA,CODIGOTIPOINGRESO,COD_ESTATUS_BANNER,ESTATUS_BANNER,SMRPRLE_PROGRAM,SPRIDEN_PIDM " +
                  ")ORDER BY 1 ";
                //Console.WriteLine(strQuery);
                //Console.ReadLine();
                //DirectoryInfo virtualDirPath = new DirectoryInfo(System.Configuration.ConfigurationManager.AppSettings["strFileName"].ToString());
                //StreamWriter sw = new StreamWriter(virtualDirPath + Globales.strFileName + ".log", true);

                //sw.WriteLine(strQuery);
                //sw.Close();
                OracleConnection ConexionBanner = new OracleConnection(ConfigurationManager.ConnectionStrings["ConexionNOAH"].ConnectionString);
                ConexionBanner.Open();
                OracleCommand Oracmd = new OracleCommand(strQuery, ConexionBanner);
                OracleDataReader DatosBanner;
                Oracmd.CommandType = CommandType.Text;
                DatosBanner = Oracmd.ExecuteReader();
                objDT.Load(DatosBanner, LoadOption.OverwriteChanges);
                ConexionBanner.Close();

                //while (DatosBanner.Read())
                //{
                //    string Matricula = DatosBanner.GetString(0);

                //    Console.WriteLine(Matricula);

                //}
                //Console.ReadLine();
                RegistrarSQLServ(objDT);


            }
            catch (Exception ex)
            {
                RegistraErrores(ex.ToString());
            }

        }

        static void Sincroniza_WA()
        {
            //OracleConnection oraCnn = new OracleConnection(ConfigurationManager.ConnectionStrings["ConexionNOAH"].ConnectionString);
            //OracleConnection oraCnn = new OracleConnection(ConfigurationManager.ConnectionStrings["ConexionNOAH_UAT"].ConnectionString);
            OracleDataReader oraDR;
            DataTable objDT = new DataTable();

            try
            {
                string Periodo = "";
                string Nivel = "";
                string Matricula = "";
                if (ConfigurationManager.AppSettings["PeriodoInicio_WA"].ToString() != "")
                {
                    Periodo = "AND SARADAP_TERM_CODE_ENTRY >= (" + ConfigurationManager.AppSettings["PeriodoInicio_WA"].ToString() + ") ";
                }
                if (ConfigurationManager.AppSettings["Niveles_WA"].ToString() != "")
                {
                    Nivel = "AND SGBSTDN_LEVL_CODE IN (" + ConfigurationManager.AppSettings["Niveles_WA"].ToString() + ") ";
                }
                if (ConfigurationManager.AppSettings["Matricula_WA"].ToString() != "")
                {
                    Matricula = "AND SPRIDEN_ID IN (" + ConfigurationManager.AppSettings["Matricula_WA"].ToString() + ")";
                }

                string strQuery = "";
                strQuery = "SELECT DISTINCT MATRICULA,NOMBRE,EMAIL1,EMAIL2,EMAIL3,EMAIL4,CAMPUS,PROGRAMA,CAMPDESC,CODIGO_AREA,NIVEL,CODIGOPROCEDENCIA,CODIGOTIPOINGRESO,COD_ESTATUS_BANNER,ESTATUS_BANNER,FECHA_1_MATERIA,FECHA_2_MATERIA,FECHA_3_MATERIA,PERIODO_ADMISION,COD_PROGRAM " +
"FROM ( " +
"SELECT DISTINCT MATRICULA,NOMBRE,EMAIL1,EMAIL2,EMAIL3,EMAIL4,CAMPUS,PROGRAMA,CAMPDESC,CODIGO_AREA,NIVEL,CODIGOPROCEDENCIA,CODIGOTIPOINGRESO,COD_ESTATUS_BANNER,ESTATUS_BANNER, " +
"MAX(CASE WHEN IDMATERIA=1 THEN SFRSTCR_RSTS_DATE ELSE NULL END) FECHA_1_MATERIA, " +
"MAX(CASE WHEN IDMATERIA=2 THEN SFRSTCR_RSTS_DATE ELSE NULL END) FECHA_2_MATERIA, " +
"MAX(CASE WHEN IDMATERIA=3 THEN SFRSTCR_RSTS_DATE ELSE NULL END) FECHA_3_MATERIA,MAX(SARADAP_TERM_CODE_ENTRY)PERIODO_ADMISION,SMRPRLE_PROGRAM COD_PROGRAM,SPRIDEN_PIDM " +
"FROM ( " +
"SELECT DISTINCT MATRICULA,NOMBRE,EMAIL1,EMAIL2,EMAIL3,EMAIL4,CAMPUS,PROGRAMA,CAMPDESC,CODIGO_AREA,NIVEL,CODIGOPROCEDENCIA,CODIGOTIPOINGRESO,COD_ESTATUS_BANNER,ESTATUS_BANNER,MAX_SGBSTDN_TERM_CODE_EFF,MATERIA,SFRSTCR_TERM_CODE,SFRSTCR_RSTS_DATE,ROW_NUMBER() OVER(PARTITION BY MATRICULA ORDER BY SFRSTCR_RSTS_DATE) IDMateria,SARADAP_TERM_CODE_ENTRY,SMRPRLE_PROGRAM,SPRIDEN_PIDM " +
"FROM( " +
 "SELECT DISTINCT SPRIDEN_ID MATRICULA, SPRIDEN_FIRST_NAME || ' ' || SPRIDEN_LAST_NAME NOMBRE, " +
                  "NVL(P1.GOREMAL_EMAIL_ADDRESS,'') EMAIL1, " +
                  "NVL(P2.GOREMAL_EMAIL_ADDRESS,'') EMAIL2, " +
                  "NVL(E1.GOREMAL_EMAIL_ADDRESS,'') EMAIL3, " +
                  "NVL(U1.GOREMAL_EMAIL_ADDRESS,'') EMAIL4, " +
                  "TO_NUMBER(SORLCUR_CAMP_CODE) Campus, SMRPRLE_PROGRAM_DESC PROGRAMA, STVCAMP_DESC CampDesc, " +
                  "CASE SMRPRLE_DEGC_CODE  WHEN  'BACHIL' THEN 1 WHEN 'LICENC' THEN 2 WHEN 'MAESTR' THEN 3  ELSE 4 END Codigo_Area, SGBSTDN_LEVL_CODE Nivel, " +
                  "SARADAP_RESD_CODE CodigoProcedencia, SARADAP_STYP_CODE CodigoTipoIngreso,SGBSTDN_STST_CODE Cod_Estatus_Banner,STVSTST_DESC Estatus_Banner,MAX_SGBSTDN_TERM_CODE_EFF,SSBSECT_SUBJ_CODE||SCBCRSE_CRSE_NUMB MATERIA,SFRSTCR_TERM_CODE,SFRSTCR_RSTS_DATE,SARADAP_TERM_CODE_ENTRY,SMRPRLE_PROGRAM,SPRIDEN_PIDM " +
                  "FROM SGBSTDN " +
                  "JOIN (  SELECT  SGBSTDN_PIDM MAX_SGBSTDN_PIDM, " +
                                 "MAX(SGBSTDN_TERM_CODE_EFF) MAX_SGBSTDN_TERM_CODE_EFF " +
                          "FROM SGBSTDN " +
                          "GROUP BY SGBSTDN_PIDM) SGBSTDN_MAX ON MAX_SGBSTDN_PIDM = SGBSTDN_PIDM AND MAX_SGBSTDN_TERM_CODE_EFF = SGBSTDN_TERM_CODE_EFF " +
                  "JOIN (SELECT DISTINCT SORLCUR_PIDM, SORLCUR_PROGRAM, SORLCUR_CAMP_CODE " +
                               "FROM SORLCUR " +
                               "WHERE SORLCUR_CACT_CODE='ACTIVE' " +
                               "AND SORLCUR_TERM_CODE=(SELECT DISTINCT MAX(X.SORLCUR_TERM_CODE) FROM SORLCUR X WHERE X.SORLCUR_PIDM=SORLCUR.SORLCUR_PIDM AND " +
                  "X.SORLCUR_CACT_CODE='ACTIVE')) ON SORLCUR_PIDM=SGBSTDN_PIDM AND SORLCUR_PROGRAM=SGBSTDN_PROGRAM_1 " +
                  "JOIN SPRIDEN ON SPRIDEN_PIDM = SGBSTDN_PIDM " +
                  "JOIN SMRPRLE ON SMRPRLE_PROGRAM = SGBSTDN_PROGRAM_1 " +
                  "JOIN STVCAMP ON STVCAMP_CODE = SORLCUR_CAMP_CODE " +
                  "JOIN SARADAP ON SARADAP_PIDM = SPRIDEN_PIDM " +
                  "JOIN SARAPPD ON SARAPPD_PIDM = SGBSTDN_PIDM AND SARAPPD_APDC_CODE IN('CA','AC','PP') " +
                  "JOIN STVSTST ON STVSTST_CODE=SGBSTDN_STST_CODE " +
                  "LEFT JOIN SFRSTCR ON SFRSTCR_PIDM=SPRIDEN_PIDM AND SFRSTCR_TERM_CODE >= MAX_SGBSTDN_TERM_CODE_EFF AND SFRSTCR_RSTS_CODE='RE' " +
                  "LEFT JOIN SSBSECT ON SSBSECT_CRN= SFRSTCR_CRN AND SSBSECT_TERM_CODE=SFRSTCR_TERM_CODE " +
                  "LEFT JOIN (SELECT DISTINCT SMRPAAP_PROGRAM,SMRACAA_AREA,SCBCRSE_CRSE_NUMB,SCBCRSE_SUBJ_CODE,SMRACAA_SEQNO,SMRPAAP_AREA_PRIORITY " +
                                    "FROM " +
                                    "SMRPAAP " +
                                    "JOIN SMRALIB ON SMRALIB_AREA = SMRPAAP_AREA " +
                                    "JOIN SMRACAA ON SMRACAA_AREA = SMRPAAP_AREA " +
                                    "JOIN SCBCRSE ON SCBCRSE_SUBJ_CODE = SMRACAA_SUBJ_CODE AND SCBCRSE_CRSE_NUMB = SMRACAA_CRSE_NUMB_LOW " +
                                    "JOIN SCRSYLN ON SCRSYLN_SUBJ_CODE = SCBCRSE_SUBJ_CODE AND SCRSYLN_CRSE_NUMB = SCBCRSE_CRSE_NUMB)ON SMRPAAP_PROGRAM=SGBSTDN_PROGRAM_1 AND SSBSECT_SUBJ_CODE=SCBCRSE_SUBJ_CODE AND SSBSECT_CRSE_NUMB=SCBCRSE_CRSE_NUMB " +
                  "LEFT JOIN GOREMAL P1 ON P1.GOREMAL_PIDM = SPRIDEN_PIDM AND P1.GOREMAL_EMAL_CODE = 'PER1' " +
                  "LEFT JOIN GOREMAL P2 ON P2.GOREMAL_PIDM = SPRIDEN_PIDM AND P2.GOREMAL_EMAL_CODE = 'PER2' " +
                  "LEFT JOIN GOREMAL E1 ON E1.GOREMAL_PIDM = SPRIDEN_PIDM AND E1.GOREMAL_EMAL_CODE = 'EMPR' " +
                  "LEFT JOIN GOREMAL U1 ON U1.GOREMAL_PIDM = SPRIDEN_PIDM AND U1.GOREMAL_EMAL_CODE = 'STAN' AND U1.GOREMAL_PREFERRED_IND='Y' " +
                  "WHERE 1 = 1 " +
                  "AND SPRIDEN_CHANGE_IND IS NULL " +
                  "AND SPRIDEN_ID LIKE 'U%' " +
                  "AND SARADAP_STYP_CODE NOT IN ('0','T','E') " +
                  Periodo +
                  Nivel +
                  Matricula +
                  ")) " +
                  "GROUP BY MATRICULA,NOMBRE,EMAIL1,EMAIL2,EMAIL3,EMAIL4,CAMPUS,PROGRAMA,CAMPDESC,CODIGO_AREA,NIVEL,CODIGOPROCEDENCIA,CODIGOTIPOINGRESO,COD_ESTATUS_BANNER,ESTATUS_BANNER,SMRPRLE_PROGRAM,SPRIDEN_PIDM " +
                  ")ORDER BY 1 ";
                //Console.WriteLine(strQuery);
                //Console.ReadLine();
                //DirectoryInfo virtualDirPath = new DirectoryInfo(System.Configuration.ConfigurationManager.AppSettings["strFileName"].ToString());
                //StreamWriter sw = new StreamWriter(virtualDirPath + Globales.strFileName + ".log", true);

                //sw.WriteLine(strQuery);
                //sw.Close();
                OracleConnection ConexionBanner = new OracleConnection(ConfigurationManager.ConnectionStrings["ConexionNOAH"].ConnectionString);
                ConexionBanner.Open();
                OracleCommand Oracmd = new OracleCommand(strQuery, ConexionBanner);
                OracleDataReader DatosBanner;
                Oracmd.CommandType = CommandType.Text;
                DatosBanner = Oracmd.ExecuteReader();
                objDT.Load(DatosBanner, LoadOption.OverwriteChanges);
                ConexionBanner.Close();

                //while (DatosBanner.Read())
                //{
                //    string Matricula = DatosBanner.GetString(0);

                //    Console.WriteLine(Matricula);

                //}
                //Console.ReadLine();
                RegistrarSQLServ(objDT);


            }
            catch (Exception ex)
            {
                RegistraErrores(ex.ToString());
            }

        }

    }
}
