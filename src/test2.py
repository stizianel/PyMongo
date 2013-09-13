'''
Created on 05/ago/2013

@author: a530966
'''
import cx_Oracle
import sys
def main():
    """ Connect to Oracle """
    username = 'SAP_USER'
    password = 'sap_user'
    host = '192.168.0.177'
    sid = 'SINPROD'
    #fileinput = os.environ['fileinput']
    conninfo = username + '/' + password + '@' + host + '/' + sid
    print conninfo

    #connessione al database
    try:
        db = cx_Oracle.connect(conninfo)
        print "------------------------------------------------------------"
        print "Benvenuto utente " + username
        print "Sei connesso a Oracle DB versione " + db.version 
        print "------------------------------------------------------------"
    except cx_Oracle.DatabaseError, exc:
        error, = exc.args
        print "------------------------------------------------------------"
        print error.message
        print "------------------------------------------------------------"
        sys.exit()
    cur = db.cursor()    
    cur.execute("""SELECT
            Id_Sr ,
            Tipologia_Sr ,
            '  '  As Id_Sr_Padre ,
            Id_Interazione ,
            TO_CHAR(Data_Apertura_Interaz,'yyyymmddhh24miss') AS Data_Apertura_Interaz ,
            Categoria_1 ,
            Categoria_2 ,
            Categoria_3 ,
            Stato_Sr ,
            Segmento_Cliente ,
            Canale_Ingaggio ,
            User_Id_Creatore ,
            TO_CHAR(Data_Creazione,'yyyymmddhh24miss') AS Data_Creazione ,
            Motivo_Stato ,
            TO_CHAR(Data_Chiusura,'yyyymmddhh24miss')     AS Data_Chiusura ,
            TO_CHAR(Data_Assegnazione,'yyyymmddhh24miss') AS Data_Assegnazione ,
            TO_CHAR(Data_Scadenza,'yyyymmddhh24miss')     AS Data_Scadenza ,
            Numero_Documenti ,
            Tipo_Corrispondenza ,
            Tipo_Servizio ,
            Note_Call_Center ,
            Note_Call_Center_2 ,
            Note_Riservate_BOS ,
            Note_Riservate_BOS_2 ,
            REPLACE(Attivita,';','')   AS Attivita ,
            REPLACE(Attivita_2,';','') AS Attivita_2 ,
            REPLACE(Attivita_3,';','') AS Attivita_3 ,
            REPLACE(Attivita_4,';','') AS Attivita_4 ,
            Identificativo_Opt ,
            Utente_Creatore ,
            Gruppo_Creatore ,
            Canale ,
            Id_Punto ,
            Punto_Fornitura ,
            Richiesta_Info ,
            Id_Ticket ,
            Codice_Cliente ,
            REPLACE(Ragione_Sociale,';','') AS Ragione_Sociale ,
            Indirizzo_Mail ,
            Indirizzo ,
            CAP ,
            Comune ,
            Provincia ,
            Telefono ,
            Fax ,
            Id_Persona_Contatto ,
            Nome_Cont ,
            Cognome_Cont ,
            Email_Cont ,
            Tel_Cont ,
            Cell_Cont ,
            Riproposizione ,
            P_Iva ,
            Cod_Fiscale ,
            Id_Ultima_Attivita ,
            TO_CHAR(Data_Ultima_Attivita,'yyyymmddhh24miss') AS Data_Ultima_Attivita
            --                                                  ## Specifici ##
            --                                                  Sr Cessazione - Ripen cliente
            ,
            Cod_Proposta
            -- Sr Cessazioni - Recesso
            ,
            TO_CHAR(Data_Cessazione,'yyyymmddhh24miss') AS Data_Cessazione
            --                                             Sr Gest Contratto - Recesso Rapido Non
            ,
            TO_CHAR(Rrng_Data_Blocco_Fatt,'yyyymmddhh24miss') AS Rrng_Data_Blocco_Fatt ,
            Rrng_Esito_Blocco ,
            TO_CHAR(Rrng_Data_Sblocco_Fatt,'yyyymmddhh24miss') AS Rrng_Data_Sblocco_Fatt ,
            Rrng_Esito_Sblocco
            -- Sr Gest Contratto - Contestazioni
            ,
            Pass_Res_Bus ,
            Pass_Bus_Res ,
            TO_CHAR(Cnt_Data_Blocco_Fatt,'yyyymmddhh24miss')  AS Cnt_Data_Blocco_Fatt ,
            TO_CHAR(Cnt_Data_Sblocco_Fatt,'yyyymmddhh24miss') AS Cnt_Data_Sblocco_Fatt ,
            Flag_Blocco
            -- Sr Gest Contratto - Mancata Fatturazione
            ,
            Flag_Letture_Ins -- Non gestito
            --                  Sr Conciliazione Paritetica
            ,
            Assegnazione ,
            Tipo_Richiedente --Non gestisto
            ,
            Id_Ass_Concil ,
            Ass_Ccl_Ragione_Sociale ,
            Ass_Ccl_Reg_Compet ,
            Ass_Ccl_Telefono ,
            Ass_Ccl_Email ,
            Ass_Ccl_Fax ,
            Id_Concil_Assoc ,
            Ccl_Ass_Nome_Cognome ,
            Ccl_Ass_Cellulare ,
            Ccl_Ass_Telefono ,
            Ccl_Ass_Email ,
            Id_Concil_Sorgenia ,
            Ccl_Sorg_Nome_Cognome ,
            Ccl_Sorg_Cellulare ,
            Ccl_Sorg_Telefono ,
            Ccl_Sorg_Email
            -- Sr Corrispondenza inesitata
            ,
            NVL(Numero_Fattura,'N.A.') ,
            TO_CHAR(Data_Mancata_Consegna,'yyyymmddhh24miss') AS Data_Mancata_Consegna ,
            Causale_Inesito ,
            Destinatario ,
            Causale_Esito_Negativo ,
            Numero_Protocollo
            -- Sr Modifica IVA
            ,
            Dato_Iva_Precedente -- Non Gestito
            ,
            Dato_Iva_Nuovo
            -- Sr Esenzione IVA
            ,
            TO_CHAR(Data_Inizio_Esenzione,'yyyymmddhh24miss') AS Data_Inizio_Esenzione ,
            TO_CHAR(Data_Fine_Esenzione,'yyyymmddhh24miss')   AS Data_Fine_Esenzione
            --                                                   Sr Pagamenti - Richiesta rimborso
            ,
            Rrmb_Cod_Entita_Fatturabile ,
            Metodo_Pagamento ,
            Rrmb_Intestatario_CC ,
            Codice_IBAN
            -- Sr Pagamenti - Revoca RID/Attivazione metodo pagamento
            ,
            Ampg_Cod_Entita_Fatturabile ,
            Modalita_Pagamento ,
            Ampg_Data_Inizio_Validita ,
            Mdp_Primario ,
            Cod_Domiciliazione ,
            Banca ,
            Filiale ,
            Cod_Nazione ,
            CIN_Europeo ,
            CIN ,
            ABI ,
            CAB ,
            Numero_CC ,
            IBAN ,
            Ampg_Intestatario_CC ,
            CF_PI_Intestatario_CC ,
            Indirizzo_Intestatario_CC ,
            Comune_Intestatario_CC ,
            CAP_Intestatario_CC ,
            Provincia_Intestatario_CC ,
            Nom_Cogn_Sottoscr_RID ,
            CF_Sottoscrittore_RID ,
            Stato_Delega ,
            Appoggio_Bancario_Disp -- Non Gestisto
            --                        Sr Pagamenti - Richiesta dilazione/rateizzazione
            ,
            Id_Piano -- Non Gestisto
            ,
            Numero_Rate -- Non Gestisto
            ,
            Frequenza_Rate -- Non Gestisto
            ,
            Intervallo_Rate -- Non Gestisto
            ,
            Piano_Rate -- Non Gestisto
            ,
            Codice_Piano_Rate -- Non Gestisto
            --                   Sr Subentro
            ,
            Sub_Rilevanza_Cont_Serv -- Non Gestisto
            ,
            Modifica_Potenza -- Non Gestisto
            ,
            Subentro_Complesso -- Non Gestisto
            ,
            TO_CHAR(Data_Desid_Subentro,'yyyymmddhh24miss') AS Data_Desid_Subentro ,
            TO_CHAR(Data_Subentro,'yyyymmddhh24miss')       AS Data_Subentro
            --                                                 Sr Mandato di connessione
            ,
            Mdc_Rilevanza_Cont_Serv -- Non Gestisto
            ,
            Mdc_Presenza_Cliente_Finale ,
            Sollevamento_Persone ,
            Intervento_Gruppo_Misura ,
            Mdc_Registrazione_Telefonica ,
            Mdc_Potenza_Richiesta ,
            Mdc_Potenza_Rich_Sup_16_5 ,
            Mdc_Potenza_Att_Impegnata ,
            Mdc_Potenza_Att_Disponibile ,
            Mdc_Franchigia -- Non Gestito
            ,
            Mdc_Tensione_Richiesta ,
            Mdc_Tensione_Attuale ,
            Mdc_Modulistica_Cliente -- Non Gestito
            ,
            Descrizione_Lavoro ,
            TO_CHAR(Data_Ricezione_Preventivo,'yyyymmddhh24miss') AS Data_Ricezione_Preventivo ,
            POD ,
            Tipo_Richiesta -- Non Gestito
            ,
            TO_CHAR(Mdc_Data_Cessione_Prevista,'yyyymmddhh24miss') AS Mdc_Data_Cessione_Prevista
            --                                                        Sr Modifica di connessione -
                                                                      -- Verifiche
            ,
            Strumento_Registrazione
            -- Sr Disattivazione
            ,
            Dis_Presenza_Cliente_Finale ,
            Disattivazione_Fuori_Orario ,
            Dis_Registrazione_Telefonica ,
            Dis_Modulistica_Cliente -- Non Gestito
            ,
            TO_CHAR(Dis_Data_Cessione_Prevista,'yyyymmddhh24miss') AS Dis_Data_Cessione_Prevista ,
            TO_CHAR(Data_Cessione_Effettiva,'yyyymmddhh24miss')    AS Data_Cessione_Effettiva ,
            Cod_Distributore_Prec
            -- Sr Reclamo
            ,
            Reclamo_Scritto ,
            TO_CHAR(Data_Ricezione_Richiesta,'yyyymmddhh24miss')    AS Data_Ricezione_Richiesta ,
            TO_CHAR(Data_Invio_Risp_Definitiva,'yyyymmddhh24miss')  AS Data_Invio_Risp_Definitiva ,
            TO_CHAR(Data_Invio_Risp_Preliminare,'yyyymmddhh24miss') AS Data_Invio_Risp_Preliminare ,
            TO_CHAR(Data_Invio_Pratica_Distr,'yyyymmddhh24miss')    AS Data_Invio_Pratica_Distr ,
            Flag_Reclamo_Complesso ,
            TO_CHAR(Data_Ricez_Risp_Distr,'yyyymmddhh24miss') AS Data_Ricez_Risp_Distr ,
            Altro_Destinatario
            -- Sr Rettifica Fatturazione
            ,
            TO_CHAR(Data_Decorrenza_Richiesta,'yyyymmddhh24miss') AS Data_Decorrenza_Richiesta -- Non
                                                                     -- Gestito
            ,
            Flag_Letture_Inserite -- Non Gestito
            ,
            Flag_Fattura_Pagata -- Non Gestito
            ,
            NVL(Rft_Cod_Fattura,'N.A.')
            -- Sr Rettifica Fatturazione - Competenza
            ,
            TO_CHAR(Data_Attivazione_Richiesta,'yyyymmddhh24miss') AS Data_Attivazione_Richiesta ,
            TO_CHAR(Data_Cessazione_Richiesta,'yyyymmddhh24miss')  AS Data_Cessazione_Richiesta ,
            NVL(Rfc_Cod_Fattura,'N.A.')
            -- Sr Rettifica Fatturazione - Mandato di connessione
            ,
            Rfmc_Potenza_Attuale -- Non Gestito
            ,
            Rfmc_Potenza_Att_Impegnata ,
            Rfmc_Potenza_Att_Disponibile ,
            Fase_Attuale ,
            Rfmc_Franchigia -- Non Gestito
            ,
            Rfmc_Tensione_Richiesta ,
            Rfmc_Tensione_Attuale ,
            Rfmc_Potenza_Richiesta ,
            Rfmc_Potenza_Rich_Sup_16_5 ,
            Fase_Richiesta
            -- Sr Rettifica Fatturazione - Formula Prezzo
            ,
            Prodotto_Applicato -- Non Gestito
            ,
            Prodotto_Richiesto -- Non Gestito
            --                    Sr Rettifica Fatturazione - Dati Fiscali
            ,
            Iva_Richiesta ,
            Accisa_Richiesta
            -- Sr Modifica - Anagrafica cliente
            ,
            Causale_Anagrafica ,
            TO_CHAR(Agf_Data_Inizio_Validita,'yyyymmddhh24miss') AS Agf_Data_Inizio_Validita ,
            Tribunale_Iscrizione ,
            Num_Reg_Tribunale ,
            Prov_Iscr_Cam_Comm ,
            Registro_Imprese ,
            Riemissione_Fattura ,
            Diniego_Marketing
        FROM
            dbi_user.IFC_SAP_CRM_SR sr,
            sapsr3.but000@sap_cap b, --partner
            sapsr3.isu_pod@sap_cap c, --ext_ui
            sapsr3.crmm_babr_h@sap_cap d --zzvkona
        WHERE
            Tipologia_Sr NOT IN ('TBD',
                                 'N.A.',
                                 'No S.R.',
                                 'ZRC1',
                                 'ZSUB')
        AND Tipologia_Sr IS NOT NULL
        AND lpad(sr.codice_cliente,10,'0') = lpad(b.partner,10,'0')
        --AND lpad(sr.id_persona_contatto,10,'0') = lpad(b.partner,10,'0')
        AND pod = c.ext_ui
        AND d.zzvkona = rrmb_cod_entita_fatturabile
        AND rownum < 10""")
    for rec in cur:
        print 'pippo'
    
if __name__ == "__main__":
    main()